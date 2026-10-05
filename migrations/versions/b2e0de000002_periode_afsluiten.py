"""periode afsluiten: closed_at, left_at, period_reports

Revision ID: b2e0de000002
Revises: a1c0de000001
"""
from alembic import op
import sqlalchemy as sa

revision = "b2e0de000002"
down_revision = "a1c0de000001"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("periods", sa.Column("closed_at", sa.DateTime(), nullable=True))
    op.add_column("users", sa.Column("left_at", sa.Date(), nullable=True))
    op.create_table(
        "period_reports",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("period_id", sa.Integer(), sa.ForeignKey("periods.id"), nullable=False, unique=True),
        sa.Column("schema_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("data", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime()),
    )
    # Bestaande niet-actieve periodes blijven closed_at = NULL: dat is "historisch, niet bevroren".
    # Er wordt bewust géén rapport gemaakt van de nu aangetaste live-data.


def downgrade():
    op.drop_table("period_reports")
    op.drop_column("users", "left_at")
    op.drop_column("periods", "closed_at")
