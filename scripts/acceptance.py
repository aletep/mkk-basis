"""Run destructive failure checks only in a newly created, isolated Compose project."""

import argparse
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from uuid import UUID, uuid4, uuid5

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-port", type=int, default=18080)
    parser.add_argument("--demo-port", type=int, default=18081)
    args = parser.parse_args()
    project = f"mkk-basis-acceptance-{uuid4().hex[:8]}"
    # The example contains local development credentials only. Do not inherit
    # deployment credentials or a different webhook origin from the caller.
    example = dict(
        line.split("=", 1)
        for line in (ROOT / ".env.example").read_text().splitlines()
        if line and not line.startswith("#")
    )
    env = os.environ | example | {"API_PORT": str(args.api_port), "DEMO_PORT": str(args.demo_port)}
    key = example["API_KEY"]
    compose = [
        "docker",
        "compose",
        "--env-file",
        str(ROOT / ".env.example"),
        "-p",
        project,
        "--profile",
        "demo",
    ]

    def dc(*command, capture=False):
        result = subprocess.run(
            compose + list(command),
            cwd=ROOT,
            env=env,
            check=True,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            timeout=240,
        )
        return result.stdout.strip() if capture else None

    def sql(statement):
        return dc(
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "payments",
            "-d",
            "payments",
            "-Atc",
            statement,
            capture=True,
        )

    def request(path, body=None, idem=None):
        headers = {"X-API-Key": key, "Content-Type": "application/json"}
        if idem:
            headers["Idempotency-Key"] = idem
        req = urllib.request.Request(
            f"http://127.0.0.1:{args.api_port}{path}",
            data=json.dumps(body).encode() if body is not None else None,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def demo_request(path, *, post=False):
        req = urllib.request.Request(
            f"http://127.0.0.1:{args.demo_port}{path}",
            data=b"" if post else None,
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            return json.load(response)

    def process_identity():
        container_id = dc("ps", "-q", "consumer", capture=True)
        assert container_id, "Consumer is not running"
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.Id}} {{.State.StartedAt}} {{.RestartCount}} {{.State.Running}}",
                container_id,
            ],
            check=True,
            text=True,
            capture_output=True,
            timeout=10,
        )
        identity = result.stdout.strip().split()
        assert identity[-1] == "true", "Consumer stopped unexpectedly"
        return identity

    def create(label, suffix=""):
        body = {
            "amount": "123.45",
            "currency": "RUB",
            "description": label,
            "metadata": {"order_id": label},
            "webhook_url": f"http://webhook-receiver:8000/webhook{suffix}",
        }
        code, response = request("/api/v1/payments", body, label)
        assert code == 202, (code, response)
        return response["payment_id"], body

    def wait_for(check, timeout=70):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = check()
            if value:
                return value
            time.sleep(0.2)
        raise AssertionError("Condition timed out")

    def completed(payment_id, exhausted=False):
        code, response = request(f"/api/v1/payments/{payment_id}")
        assert code == 200
        return (
            response
            if response["processing_exhausted" if exhausted else "webhook_delivered_at"]
            else None
        )

    def blocked_webhook(payment_id, gate):
        barrier = demo_request(f"/gates/{gate}")
        if not barrier["entered"]:
            return None
        assert barrier == {"entered": 1, "released": False}, barrier
        code, saved = request(f"/api/v1/payments/{payment_id}")
        assert code == 200
        # A failure must be injected while the first webhook is actually waiting.
        # Reaching this helper after completion is a failed precondition, never PASS.
        assert saved["processed_at"] and saved["processing_attempts"] == 1, saved
        assert not saved["webhook_delivered_at"] and not saved["processing_exhausted"], saved
        return saved

    def passed(label):
        print(f"PASS: {label}", flush=True)

    print(f"Isolated project: {project}", flush=True)
    try:
        dc("config", "--quiet")
        dc("up", "-d", "--build", "--wait")
        assert dc("exec", "-T", "api", "id", "-u", capture=True) == "10001"
        passed("empty volumes, migrations, healthchecks, non-root runtime")
        payment_id, body = create("smoke")
        saved = wait_for(lambda: completed(payment_id))
        assert saved["status"] in ("succeeded", "failed") and saved["processing_attempts"] == 1
        assert request("/api/v1/payments", body, "smoke")[1]["payment_id"] == payment_id
        assert request("/api/v1/payments", body | {"amount": "2"}, "smoke")[0] == 409
        passed("real gateway, HTTP webhook, idempotency and conflict")

        retry_id, _ = create("retry", "?failures=2")
        assert wait_for(lambda: completed(retry_id))["processing_attempts"] == 3
        failed_id, _ = create("exhausted", "?status=500")
        failed = wait_for(lambda: completed(failed_id, exhausted=True))
        assert failed["processing_attempts"] == 3 and failed["processed_at"]
        wait_for(
            lambda: (
                sql(
                    "SELECT published_at IS NOT NULL FROM outbox WHERE payment_id='"
                    + failed_id
                    + "'"
                )
                == "t"
            )
        )

        def dead_letter_ready():
            queues = json.loads(
                dc(
                    "exec",
                    "-T",
                    "rabbitmq",
                    "rabbitmqctl",
                    "list_queues",
                    "name",
                    "type",
                    "messages_ready",
                    "arguments",
                    "--formatter=json",
                    capture=True,
                )
            )
            by_name = {item["name"]: item for item in queues}
            return by_name if by_name["payments.dlq"]["messages_ready"] == 1 else None

        by_name = wait_for(dead_letter_ready)
        assert by_name["payments.new"]["type"] == "quorum"
        passed("HTTP 500/500/200, exhausted budget, quorum queues and DLQ")

        broker_identity = process_identity()
        dc("stop", "-t", "0", "rabbitmq")
        outage_id, _ = create("broker-outage")
        assert request("/health")[0] == 200
        assert sql(f"SELECT published_at IS NULL FROM outbox WHERE payment_id='{outage_id}'") == "t"
        dc("up", "-d", "--wait", "rabbitmq")
        assert wait_for(lambda: completed(outage_id))["processing_attempts"] == 1
        assert process_identity() == broker_identity, "Consumer restarted during broker outage"
        passed("broker outage accepts POST; reconnect drains outbox without process restart")

        database_id, _ = create("database-outage", "?gate=database-outage")
        before_database = wait_for(lambda: blocked_webhook(database_id, "database-outage"))
        database_identity = process_identity()
        dc("stop", "-t", "0", "postgres")
        assert request(f"/api/v1/payments/{database_id}")[0] == 503
        demo_request("/gates/database-outage/release", post=True)
        wait_for(
            lambda: any(
                event["payment_id"] == database_id for event in demo_request("/events")["events"]
            )
        )
        dc("up", "-d", "--wait", "postgres")
        after_database = wait_for(lambda: completed(database_id))
        assert after_database["processing_attempts"] == before_database["processing_attempts"] == 1
        assert after_database["processed_at"] == before_database["processed_at"]
        assert process_identity() == database_identity, "Consumer restarted during database outage"
        passed("database loss during webhook waits for recovery without a new attempt or restart")

        restart_id, _ = create("consumer-restart", "?status=500&gate=consumer-restart")
        before = wait_for(lambda: blocked_webhook(restart_id, "consumer-restart"))
        crash_identity = process_identity()
        dc("stop", "-t", "0", "consumer")
        stopped = request(f"/api/v1/payments/{restart_id}")[1]
        assert stopped["processing_attempts"] == before["processing_attempts"] == 1
        assert stopped["processed_at"] == before["processed_at"]
        assert not stopped["webhook_delivered_at"] and not stopped["processing_exhausted"]
        demo_request("/gates/consumer-restart/release", post=True)
        dc("up", "-d", "consumer")
        after = wait_for(lambda: completed(restart_id, exhausted=True))
        assert process_identity() != crash_identity, "Consumer was not restarted by the scenario"
        assert after["processing_attempts"] == 3
        assert before["processed_at"] == after["processed_at"]
        # The killed first attempt counts; only two further HTTP calls are allowed.
        receiver = demo_request("/events")
        assert receiver["attempts"][str(uuid5(UUID(restart_id), "payment.completed"))] == 3
        passed("consumer crash and redelivery preserve result and attempt budget")

        dc("stop", "api", "consumer")
        dc("restart", "postgres", "rabbitmq")
        dc("up", "-d", "--wait", "api", "consumer")
        assert request("/api/v1/payments", body, "smoke")[1]["payment_id"] == payment_id
        assert completed(payment_id)["webhook_delivered_at"] == saved["webhook_delivered_at"]
        passed("volumes and completed stages survive service restart")
    finally:
        # UUID project is created above; cleanup cannot target another installation.
        dc("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
