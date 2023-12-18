from pathlib import Path

from sqlalchemy.orm import (
    Mapped,
    DeclarativeBase,
    mapped_column,
    Session,
)
from sqlalchemy import (
    ForeignKey,
    create_engine,
)
import pandas as pd


FILEPATH = Path(__file__).parent / Path('titanic.csv')


class Base(DeclarativeBase):
    pass


class Passenger(Base):

    __tablename__ = 'passengers'

    id: Mapped[int] = mapped_column(primary_key=True)
    survived: Mapped[bool]
    p_class: Mapped[int]
    name: Mapped[str]
    sex: Mapped[int] = mapped_column(ForeignKey('sex.id'))
    age: Mapped[float] = mapped_column(nullable=True)
    sib_sp: Mapped[int] = mapped_column(nullable=True)
    parch: Mapped[int] = mapped_column(nullable=True)
    ticket: Mapped[str] = mapped_column(nullable=True)
    fare: Mapped[float] = mapped_column(nullable=True)
    cabin: Mapped[str] = mapped_column(nullable=True)
    embarked: Mapped[str] = mapped_column(nullable=True)


class Sex(Base):

    __tablename__ = 'sex'

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)


engine = create_engine('postgresql+psycopg2://admin:password@127.0.0.1/db')

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)


with Session(engine) as session:

    df = pd.read_csv(FILEPATH)
    objects = []

    # Adding unique values to another table
    session.add_all(instances=[
        Sex(name=sex) for sex in df['Sex'].unique()
    ])
    session.commit()

    for index, row in df.iterrows():
        sex = session.query(Sex).where(Sex.name == row['Sex']).first().id
        objects.append(
            Passenger(
                id=row['PassengerId'],
                survived=row['Survived'],
                p_class=row['Pclass'],
                name=row['Name'],
                sex=sex,
                age=row['Age'],
                sib_sp=row['SibSp'],
                parch=row['Parch'],
                ticket=row['Ticket'],
                fare=row['Fare'],
                cabin=row['Cabin'],
                embarked=row['Embarked']
            )
        )

    session.add_all(instances=objects)
    session.commit()
