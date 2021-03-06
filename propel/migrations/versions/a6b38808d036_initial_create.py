"""Initial Create

Revision ID: a6b38808d036
Revises:
Create Date: 2018-07-22 16:11:15.478927

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'a6b38808d036'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        'connections',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('type', sa.Enum('Twitter', 'Youtube', 'NewsAPI'), nullable=False),
        sa.Column('key', sa.String(length=5000), nullable=True),
        sa.Column('secret', sa.String(length=5000), nullable=True),
        sa.Column('token', sa.String(length=5000), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_table(
        'news',
        sa.Column('news_id', mysql.BIGINT(unsigned=True), nullable=False),
        sa.Column('author', sa.String(length=100), nullable=True),
        sa.Column('description', sa.String(length=2000), nullable=True),
        sa.Column('published_at', sa.DateTime(), nullable=True),
        sa.Column('source', sa.JSON(), nullable=True),
        sa.Column('title', sa.String(length=2000), nullable=True),
        sa.Column('url', sa.String(length=1000), nullable=True),
        sa.Column('raw_news', sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint('news_id')
    )
    op.create_table(
        'task_groups',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('is_enabled', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_table(
        'tasks',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('task_name', sa.String(length=1000), nullable=False),
        sa.Column('task_type', sa.Enum('TwitterExtract', 'NewsDownload'), nullable=False),
        sa.Column('task_args', sa.String(length=1000), nullable=False),
        sa.Column('run_frequency_seconds', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_table(
        'tweets',
        sa.Column('tweet_id', mysql.BIGINT(unsigned=True), nullable=False),
        sa.Column('tweet_type', sa.String(length=255), nullable=True),
        sa.Column('tweet_created_at', sa.DateTime(), nullable=True),
        sa.Column('text', sa.String(length=1000), nullable=True),
        sa.Column('media_urls', sa.JSON(), nullable=True),
        sa.Column('media_types', sa.JSON(), nullable=True),
        sa.Column('media_video_urls', sa.JSON(), nullable=True),
        sa.Column('expanded_urls', sa.JSON(), nullable=True),
        sa.Column('favorite_count', sa.Integer(), nullable=True),
        sa.Column('retweet_count', sa.Integer(), nullable=True),
        sa.Column('user_id', sa.BigInteger(), nullable=True),
        sa.Column('user_screen_name', sa.String(length=100), nullable=True),
        sa.Column('in_reply_to_tweet_id', mysql.BIGINT(unsigned=True), nullable=True),
        sa.Column('in_reply_to_user_id', sa.BigInteger(), nullable=True),
        sa.Column('in_reply_to_screen_name', sa.String(length=100), nullable=True),
        sa.Column('quoted_tweet_id', mysql.BIGINT(unsigned=True), nullable=True),
        sa.Column('quoted_created_at', sa.DateTime(), nullable=True),
        sa.Column('quoted_text', sa.String(length=1000), nullable=True),
        sa.Column('quoted_media_urls', sa.JSON(), nullable=True),
        sa.Column('quoted_media_types', sa.JSON(), nullable=True),
        sa.Column('quoted_media_video_urls', sa.JSON(), nullable=True),
        sa.Column('quoted_expanded_urls', sa.JSON(), nullable=True),
        sa.Column('quoted_favorite_count', sa.Integer(), nullable=True),
        sa.Column('quoted_retweet_count', sa.Integer(), nullable=True),
        sa.Column('quoted_user_id', sa.BigInteger(), nullable=True),
        sa.Column('quoted_user_screen_name', sa.String(length=100), nullable=True),
        sa.Column('retweet_tweet_id', mysql.BIGINT(unsigned=True), nullable=True),
        sa.Column('retweet_created_at', sa.DateTime(), nullable=True),
        sa.Column('retweet_text', sa.String(length=1000), nullable=True),
        sa.Column('retweet_media_urls', sa.JSON(), nullable=True),
        sa.Column('retweet_media_types', sa.JSON(), nullable=True),
        sa.Column('retweet_media_video_urls', sa.JSON(), nullable=True),
        sa.Column('retweet_expanded_urls', sa.JSON(), nullable=True),
        sa.Column('retweet_favorite_count', sa.Integer(), nullable=True),
        sa.Column('retweet_retweet_count', sa.Integer(), nullable=True),
        sa.Column('retweet_user_id', sa.BigInteger(), nullable=True),
        sa.Column('retweet_user_screen_name', sa.String(length=100), nullable=True),
        sa.Column('raw_tweet', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('tweet_id')
    )
    op.create_table(
        'task_group_members',
        sa.Column('task_group_id', sa.Integer(), nullable=True),
        sa.Column('task_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['task_group_id'], ['task_groups.id'], ),
        sa.ForeignKeyConstraint(['task_id'], ['tasks.id'], )
    )
    op.create_table(
        'task_runs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('task_id', sa.Integer(), nullable=False),
        sa.Column('state', sa.String(length=255), nullable=False),
        sa.Column('run_ds', sa.DateTime(), nullable=False),
        sa.Column('start_time', sa.DateTime(), nullable=True),
        sa.Column('end_time', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['task_id'], ['tasks.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_table(
        'heartbeats',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('task_run_id', sa.Integer(), nullable=True),
        sa.Column('task_type', sa.String(length=255), nullable=False),
        sa.Column('heartbeat_start_time', sa.DateTime(), nullable=True),
        sa.Column('last_heartbeat_time', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['task_run_id'], ['task_runs.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('heartbeats')
    op.drop_table('task_runs')
    op.drop_table('task_group_members')
    op.drop_table('tweets')
    op.drop_table('tasks')
    op.drop_table('task_groups')
    op.drop_table('news')
    op.drop_table('connections')
    # ### end Alembic commands ###
