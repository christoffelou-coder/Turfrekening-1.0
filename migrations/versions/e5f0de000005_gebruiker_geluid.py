"""gebruiker geluid: users.sound_url

Alleen een lege kolom erbij; de oude code blijft werken.

Revision ID: e5f0de000005
Revises: d4f0de000004
"""
from alembic import op
import sqlalchemy as sa

revision = "e5f0de000005"
down_revision = "d4f0de000004"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("users", sa.Column("sound_url", sa.Text(), nullable=True))


def downgrade():
    with op.batch_alter_table("users") as batch:
        batch.drop_column("sound_url")
