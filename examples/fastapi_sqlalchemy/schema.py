import graphene
from graphene import relay

from graphene_sqlalchemy import SQLAlchemyConnectionField, SQLAlchemyObjectType
from graphene_sqlalchemy.node import AsyncNode
from models import (
    Department as DepartmentModel,
    Employee as EmployeeModel,
    Role as RoleModel,
)


class Department(SQLAlchemyObjectType):
    class Meta:
        model = DepartmentModel
        interfaces = (AsyncNode,)


class Employee(SQLAlchemyObjectType):
    class Meta:
        model = EmployeeModel
        interfaces = (AsyncNode,)


class Role(SQLAlchemyObjectType):
    class Meta:
        model = RoleModel
        interfaces = (AsyncNode,)


class Query(graphene.ObjectType):
    node = relay.Node.Field()
    # Allow only single column sorting
    all_employees = SQLAlchemyConnectionField(
        Employee.connection, sort=Employee.sort_argument()
    )
    # Allows sorting over multiple columns, by default over the primary key
    all_roles = SQLAlchemyConnectionField(Role.connection)
    # Disable sorting over this field
    all_departments = SQLAlchemyConnectionField(Department.connection, sort=None)


schema = graphene.Schema(query=Query)
