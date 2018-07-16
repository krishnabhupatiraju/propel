from flask_wtf import FlaskForm
from wtforms import TextAreaField, DateTimeField, SubmitField
from wtforms.validators import DataRequired


class ArticlesDeck(FlaskForm):
    authors = TextAreaField('Authors', validators=[DataRequired()])
    start_dt = DateTimeField(
        'StartDateTime',
        validators=[DataRequired()],
        format='%Y-%m-%d %H:%M:%S'
    )
    end_dt = DateTimeField(
        'EndDateTime',
        validators=[DataRequired()],
        format='%Y-%m-%d %H:%M:%S'
    )
    submit = SubmitField()
