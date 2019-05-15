from peewee import SqliteDatabase, Model, CharField, IntegerField, ForeignKeyField

DATABASE = 'data.db'


database = SqliteDatabase(DATABASE)


class BaseModel(Model):
    class Meta:
        database = database


class Apple(BaseModel):
    table = CharField()  # table name or id
    column = IntegerField()  # column order (position)
    total = IntegerField()  # Total number of bites/slices


class Bite(BaseModel):
    slice = IntegerField()  # slice order (position)
    apple = ForeignKeyField(Apple, backref='bites')


def create_tables():
    with database:
        database.create_tables([Apple, Bite, ])




