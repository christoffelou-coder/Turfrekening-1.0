"""product geluid: sound_url, sound_every

Alleen toevoegen van twee lege kolommen; oude code blijft werken.

Revision ID: d4f0de000004
Revises: c3f0de000003
"""
from alembic import op
import sqlalchemy as sa

revision = "d4f0de000004"
down_revision = "c3f0de000003"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("products", sa.Column("sound_url", sa.Text(), nullable=True))
    op.add_column("products", sa.Column("sound_every", sa.Integer(), nullable=True))
    op.execute("UPDATE products SET sound_every = 3")


def downgrade():
    with op.batch_alter_table("products") as batch:
        batch.drop_column("sound_every")
        batch.drop_column("sound_url")
