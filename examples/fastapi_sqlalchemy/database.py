from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool


engine = create_async_engine(
    "postgresql+asyncpg://godunov:godunov@127.0.0.1:5432/test_graphene",
    convert_unicode=True,
    echo=True,
    # https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html#using-multiple-asyncio-event-loops
    poolclass=NullPool,
)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine, class_=AsyncSession)
)
Base = declarative_base()


async def init_db():
    # import all modules here that might define models so that
    # they will be registered properly on the metadata.  Otherwise
    # you will have to import them first before calling init_db()
    from models import Department, Employee, Role

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with db_session() as session:
        async with session.begin():
            # Create the fixtures
            engineering = Department(name="Engineering")
            db_session.add(engineering)
            hr = Department(name="Human Resources")
            db_session.add(hr)

            manager = Role(name="manager")
            db_session.add(manager)
            engineer = Role(name="engineer")
            db_session.add(engineer)

            peter = Employee(name="Peter", department=engineering, role=engineer)
            db_session.add(peter)
            roy = Employee(name="Roy", department=engineering, role=engineer)
            db_session.add(roy)
            tracy = Employee(name="Tracy", department=hr, role=manager)
            db_session.add(tracy)
            await db_session.commit()
