from flask_wtf import FlaskForm
from wtforms import StringField, DateTimeField, SubmitField, HiddenField, ValidationError
from wtforms.fields.html5 import DateTimeLocalField
from wtforms.validators import Regexp, Optional, DataRequired
from db.entities.application import ApplicationEntity


class AbstractForm(FlaskForm):
    """
    AbstractForm class
    """
    def __init__(self, session=None):
        """
        Create an AbstractForm
        :param session:
        """
        super(AbstractForm, self).__init__()
        self.session = session

    def is_identical(self, attr_1, attr_2):
        """
        Check if the content of the two fields is identical
        :param attr_1: Field
        :param attr_2: Field
        :return: True if identical, otherwise False
        """
        if attr_1.data == attr_2.data:
            attr_2.errors.append(f"Cannot be identical to {attr_1.label.text}.")
            return True
        return False

    def all_exist(self, entity, inputs):
        """
        Check if the values submitted to the Field can be found in the database in the respective entity
        :param entity: entity to be checked
        :param inputs: dictionary {attribute_name: Field}
        :return: True if all the inputs exist in the database. False otherwise
        """
        invalid_inputs = []

        for i in inputs:
            if len(i) != 1:  # this should have length of 1, otherwise the app can behave incorrectly
                raise ValueError(f"Invalid format of input validation dict. Its length should be 1, got {i}")

            filter = {k: v.data for k, v in i.items()}
            if self.session.query(entity).filter_by(**filter).first() is None:
                invalid_inputs.append(list(i.values())[0])
        cnt = 0
        for ii in invalid_inputs:
            cnt += 1
            ii.errors.append(f"'{ii.data}': No such record found.")
            if cnt == len(invalid_inputs):
                return False
        return True


class SearchForm(AbstractForm):
    """
    Form associated to the Search page
    """
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
        """
        Validate the SearchForm
        :return: True if valid. False otherwise.
        """
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
        """
        Filter the results of the existing query by Form criteria
        :param query: SQLAlchemy query
        :return: filtered query
        """
        query = query.filter(ApplicationEntity.name.like(f"%{self.app_name.data.strip()}%")) \
                     .filter(ApplicationEntity.app_id.like(f"%{self.app_id.data.strip()}%")) \
                     .filter(ApplicationEntity.spark_user.like(f"%{self.username.data.strip()}%"))
        query = query.filter(ApplicationEntity.start_time >= self.start_from.data) if self.start_from.data else query
        query = query.filter(ApplicationEntity.start_time <= self.start_to.data) if self.start_to.data else query
        query = query.filter(ApplicationEntity.end_time >= self.end_from.data) if self.end_from.data else query
        query = query.filter(ApplicationEntity.end_time <= self.end_to.data) if self.end_to.data else query
        return query


class CompareForm(AbstractForm):
    """
    Form associated to the Compare page
    """
    app_id_1 = StringField("Application ID 1", validators=[DataRequired()])
    app_id_2 = StringField("Application ID 2", validators=[DataRequired()])
    compare_btn = SubmitField("Compare")

    def validate(self):
        """
        Validate the CompareForm
        :return: True if valid. False otherwise.
        """
        rv = FlaskForm.validate(self)
        check_attr = [{'app_id': self.app_id_1},
                      {'app_id': self.app_id_2}]
        if not rv \
                or self.is_identical(self.app_id_1, self.app_id_2) \
                or not self.all_exist(ApplicationEntity, check_attr):
            return False
        return True


class HistoryForm(AbstractForm):
    """
    Form associated to the History page
    """
    app_name = StringField("Application Name", validators=[DataRequired()])
    history_btn = SubmitField("See History")

    def validate(self):
        """
        Validate the HistoryForm
        :return: True if valid. False otherwise.
        """
        rv = FlaskForm.validate(self)
        check_attr = [{'name': self.app_name}]
        if not rv or not self.all_exist(ApplicationEntity, check_attr):
            return False
        return True

