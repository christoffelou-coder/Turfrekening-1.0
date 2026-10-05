"""bedragen in hele centen, prijs per turfje

Alle geldbedragen gaan van Float (euro) naar Integer (centen). Tallies krijgen
unit_price_cents (gevuld met de huidige productprijs). Draai vooraf een backup.

Revision ID: a1c0de000001
Revises: 7615fb1eb8e0
"""
from alembic import op
import sqlalchemy as sa

revision = "a1c0de000001"
down_revision = "7615fb1eb8e0"
branch_labels = None
depends_on = None

# (tabel, oude kolom, nieuwe kolom, nullable)
COLUMNS = [
    ("products", "price", "price_cents", False),
    ("payments", "amount", "amount_cents", False),
    ("corrections", "amount", "amount_cents", False),
    ("ho_events", "total_cost", "total_cost_cents", False),
    ("ho_event_shares", "amount", "amount_cents", False),
    ("inventory_purchases", "total_cost", "total_cost_cents", True),
    ("period_start_balances", "balance", "balance_cents", False),
]


def upgrade():
    for table, old, new, nullable in COLUMNS:
        op.add_column(table, sa.Column(new, sa.Integer(), nullable=True))
        op.execute(f"UPDATE {table} SET {new} = CAST(ROUND({old} * 100) AS INTEGER) WHERE {old} IS NOT NULL")
        with op.batch_alter_table(table) as batch:
            if not nullable:
                batch.alter_column(new, existing_type=sa.Integer(), nullable=False)
            batch.drop_column(old)

    op.add_column("tallies", sa.Column("unit_price_cents", sa.Integer(), nullable=True))
    op.execute(
        "UPDATE tallies SET unit_price_cents = "
        "(SELECT price_cents FROM products WHERE products.id = tallies.product_id)"
    )
    with op.batch_alter_table("tallies") as batch:
        batch.alter_column("unit_price_cents", existing_type=sa.Integer(), nullable=False)


def downgrade():
    with op.batch_alter_table("tallies") as batch:
        batch.drop_column("unit_price_cents")

    for table, old, new, nullable in COLUMNS:
        op.add_column(table, sa.Column(old, sa.Float(), nullable=True))
        op.execute(f"UPDATE {table} SET {old} = {new} / 100.0 WHERE {new} IS NOT NULL")
        with op.batch_alter_table(table) as batch:
            if not nullable:
                batch.alter_column(old, existing_type=sa.Float(), nullable=False)
            batch.drop_column(new)
