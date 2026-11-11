"""Preserve an attempt across an uncertain database commit."""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("payments", sa.Column("processing_token", sa.Uuid()))


def downgrade() -> None:
    op.drop_column("payments", "processing_token")
