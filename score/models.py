from peewee import SqliteDatabase, Model, CharField, IntegerField
from peewee import *

# DATABASE = ':memory:'
DATABASE = 'data.db'


database = SqliteDatabase(DATABASE)


class BaseModel(Model):
    class Meta:
        database = database


class Bite(BaseModel):
    table = CharField()  # table name or id
    column = IntegerField()  # column order (position)
    slice = IntegerField()  # slice order (position)
    addr = CharField()  # the url:ip of the server that the processed data need to be sent to


def create_tables():
    with database:
        database.create_tables([Bite, ])