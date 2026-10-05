"""users.previous_balance weg

Beginstanden staan sinds het afsluiten-model alleen nog in period_start_balances
(eindstand vorige periode). Draai vooraf een backup: de waarden in deze kolom
gaan verloren (downgrade zet de kolom terug met 0).

Revision ID: c3f0de000003
Revises: b2e0de000002
"""
from alembic import op
import sqlalchemy as sa

revision = "c3f0de000003"
down_revision = "b2e0de000002"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("users") as batch:
        batch.drop_column("previous_balance")


def downgrade():
    op.add_column("users", sa.Column("previous_balance", sa.Float(), nullable=True, server_default="0"))
