"""add posting buckets

Revision ID: de0524351acb
Revises: 36c8731a092a
Create Date: 2026-09-29 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'de0524351acb'
down_revision: Union[str, Sequence[str], None] = '36c8731a092a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('posting_buckets',
    sa.Column('user_id', sa.String(length=64), nullable=False),
    sa.Column('full_at', sa.DateTime(timezone=True), nullable=True),
    sa.PrimaryKeyConstraint('user_id')
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('posting_buckets')
