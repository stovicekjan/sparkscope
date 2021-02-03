from flask_wtf import FlaskForm
from wtforms import StringField, DateTimeField, SubmitField, HiddenField, ValidationError
from wtforms.fields.html5 import DateTimeLocalField
from wtforms.validators import Regexp, Optional, DataRequired
from db.entities.application import Application


class AbstractForm(FlaskForm):
    def __init__(self, session=None):
        super(AbstractForm, self).__init__()
        self.session = session

    def is_identical(self, attr_1, attr_2):
        if attr_1.data == attr_2.data:
            attr_2.errors.append(f"Cannot be identical to {attr_1.label.text}.")
            return True
        return False

    def all_exist(self, entity, inputs):
        invalid_inputs = [i for i in inputs if self.session.query(entity).filter_by(app_id=i.data).first() is None]
        cnt = 0
        for ii in invalid_inputs:
            cnt += 1
            ii.errors.append(f"'{ii.data}': No such ID found.")
            if cnt == len(invalid_inputs):
                return False
        return True


class SearchForm(AbstractForm):
    fmt = '%Y-%m-%dT%H:%M'
    app_name = StringField("Application Name", validators=[Optional(strip_whitespace=True), Regexp(regex='^[a-zA-Z0-9\-_]*$', message="Only letters, numbers, _ or - allowed.")])
    app_id = StringField("Application ID", validators=[Optional(strip_whitespace=True), Regexp(regex='^[a-zA-Z0-9\-_]*$', message="Only letters, numbers, _ or - allowed.")])
    username = StringField("Username", validators=[Optional(strip_whitespace=True), Regexp(regex='^[a-zA-Z0-9\-_]*$', message="Only letters, numbers, _ or - allowed.")])
    start_time = HiddenField("Start Time")  # just for the label
    start_from = DateTimeLocalField("From", format=fmt, validators=[Optional()])
    start_to = DateTimeLocalField("To", format=fmt, validators=[Optional()])
    end_time = HiddenField("End Time")  # just for the label
    end_from = DateTimeLocalField("From", format=fmt, validators=[Optional()])
    end_to = DateTimeLocalField("To", format=fmt, validators=[Optional()])
    search_btn = SubmitField("Search")

    def validate(self):
        rv = FlaskForm.validate(self)
        if not rv:
            return False
        # validate the dates (from < to)
        if self.start_from.data and self.start_to.data:
            if self.start_from.data > self.start_to.data:
                self.start_time.errors.append(f"Invalid From/To datetimes: '{self.start_from.label.text}' should be before '{self.start_to.label.text}'")
                return False
        if self.end_from.data and self.end_to.data:
            if self.end_from.data > self.end_to.data:
                self.end_time.errors.append(f"Invalid From/To datetimes: '{self.end_from.label.text}' should be before '{self.end_to.label.text}'")
                return False
        return True

    def apply_filters(self, query):
        query = query.filter(Application.name.like(f"%{self.app_name.data}%")) \
                     .filter(Application.app_id.like(f"%{self.app_id.data}%")) \
                     .filter(Application.spark_user.like(f"%{self.username.data}%"))
        query = query.filter(Application.start_time >= self.start_from.data) if self.start_from.data else query
        query = query.filter(Application.start_time <= self.start_to.data) if self.start_to.data else query
        query = query.filter(Application.end_time >= self.end_from.data) if self.end_from.data else query
        query = query.filter(Application.end_time <= self.end_to.data) if self.end_to.data else query
        return query

class CompareForm(AbstractForm):
    app_id_1 = StringField("Application ID 1", validators=[DataRequired()])
    app_id_2 = StringField("Application ID 2", validators=[DataRequired()])
    compare_btn = SubmitField("Compare")

    def validate(self):
        rv = FlaskForm.validate(self)
        if not rv \
                or self.is_identical(self.app_id_1, self.app_id_2) \
                or not self.all_exist(Application, [self.app_id_1, self.app_id_2]):
            return False
        return True


class HistoryForm(AbstractForm):
    app_id = StringField("Application ID", validators=[DataRequired()])
    history_btn = SubmitField("See History")

    def validate(self):
        rv = FlaskForm.validate(self)
        if not rv or not self.all_exist(Application, [self.app_id]):
            return False
        return True
