from flask_wtf import FlaskForm
from wtforms import TextAreaField, DateTimeField, SubmitField
from wtforms.validators import DataRequired


class TweetsDeck(FlaskForm):
    user_screen_names = TextAreaField('UserScreenNames', validators=[DataRequired()])
    tweets_start_dt = DateTimeField(
        'TweetStartDateTime',
        validators=[DataRequired()],
        format='%Y-%m-%d %H:%M:%S'
    )
    tweets_end_dt = DateTimeField(
        'TweetStartDateTime',
        validators=[DataRequired()],
        format='%Y-%m-%d %H:%M:%S'
    )
    submit = SubmitField()

