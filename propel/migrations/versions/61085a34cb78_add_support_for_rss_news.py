"""Add support for RSS News

Revision ID: 61085a34cb78
Revises: aa83a25570e9
Create Date: 2019-01-09 23:46:20.719318

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '61085a34cb78'
down_revision = 'aa83a25570e9'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        'dags',
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('description', sa.String(length=1000), nullable=True),
        sa.Column('is_continuous_refresh', sa.Boolean(), nullable=True),
        sa.Column('is_scheduled', sa.Boolean(), nullable=True),
        sa.Column('start_date', sa.DateTime(), nullable=True),
        sa.Column('interval', sa.Interval(), nullable=True),
        sa.Column('dag_json', sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint('name')
    )
    op.add_column(u'news', sa.Column('summary', sa.String(length=2000), nullable=True))
    op.alter_column(u'news', 'news_id',
               existing_type=mysql.BIGINT(display_width=20, unsigned=True),
               type_=sa.String(length=64),
               nullable=False)
    op.alter_column(u'news', 'source',
               existing_type=mysql.JSON(),
               type_=sa.String(length=100),
               existing_nullable=True)
    op.drop_column(u'news', 'description')
    op.drop_column(u'news', 'author')
    op.alter_column(u'task_groups', 'is_enabled',
               existing_type=mysql.TINYINT(display_width=1),
               type_=sa.Boolean(),
               existing_nullable=True)
    op.alter_column(u'tasks', 'schedule_latest',
               existing_type=mysql.TINYINT(display_width=1),
               type_=sa.Boolean(),
               existing_nullable=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(u'tasks', 'schedule_latest',
               existing_type=sa.Boolean(),
               type_=mysql.TINYINT(display_width=1),
               existing_nullable=True)
    op.alter_column(u'task_groups', 'is_enabled',
               existing_type=sa.Boolean(),
               type_=mysql.TINYINT(display_width=1),
               existing_nullable=True)
    op.add_column(u'news', sa.Column('author', mysql.VARCHAR(length=100), nullable=True))
    op.add_column(u'news', sa.Column('description', mysql.VARCHAR(length=2000), nullable=True))
    op.alter_column(u'news', 'source',
               existing_type=sa.String(length=100),
               type_=mysql.JSON(),
               existing_nullable=True)
    op.alter_column(u'news', 'news_id',
               existing_type=sa.String(length=64),
               type_=mysql.BIGINT(display_width=20, unsigned=True),
               nullable=False)
    op.drop_column(u'news', 'summary')
    op.drop_table('dags')
    # ### end Alembic commands ###
