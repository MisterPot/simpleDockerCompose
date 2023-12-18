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


def init_db() -> None:
    Base.metadata.create_all(engine)


def create_or_update(row: pd.Series, session: Session) -> Passenger:

    passenger_id = row['PassengerId']
    passenger = session.get(Passenger, passenger_id)
    sex = session.query(Sex).where(Sex.name == row['Sex']).first().id

    values = {
        'survived': row['Survived'],
        'p_class': row['Pclass'],
        'name': row['Name'],
        'sex': sex,
        'age': row['Age'],
        'sib_sp': row['SibSp'],
        'parch': row['Parch'],
        'ticket': row['Ticket'],
        'fare': row['Fare'],
        'cabin': row['Cabin'],
        'embarked': row['Embarked']
    }

    if not passenger:
        passenger = Passenger(id=passenger_id, **values)

    else:
        for key, value in values.items():
            if getattr(passenger, key) != value:
                setattr(passenger, key, value)

    return passenger


def main() -> None:

    with Session(engine) as session:
        df = pd.read_csv(FILEPATH)
        objects = []

        current_uniques = [
            item[0] for item in session.query(Sex.name).distinct().all()
        ]
        unique_items = [
            item for item in df['Sex'].unique()
            if item not in current_uniques
        ]

        # Adding unique values to another table
        session.add_all(instances=[
            Sex(name=sex) for sex in unique_items
        ])
        session.commit()

        for _, row in df.iterrows():
            objects.append(
                create_or_update(row, session)
            )

        session.add_all(instances=objects)
        session.commit()


if __name__ == '__main__':
    init_db()
    main()
