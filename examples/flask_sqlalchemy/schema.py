from models import Department as DepartmentModel
from models import Employee as EmployeeModel
from models import Role as RoleModel

import graphene
from graphene import relay
from graphene_sqlalchemy_core import SortableSQLAlchemyConnectionField, SQLAlchemyObjectType


class Department(SQLAlchemyObjectType):
    class Meta:
        model = DepartmentModel
        interfaces = (relay.Node,)


class Employee(SQLAlchemyObjectType):
    class Meta:
        model = EmployeeModel
        interfaces = (relay.Node,)


class Role(SQLAlchemyObjectType):
    class Meta:
        model = RoleModel
        interfaces = (relay.Node,)


class Query(graphene.ObjectType):
    node = relay.Node.Field()
    # Allow only single column sorting
    all_employees = SortableSQLAlchemyConnectionField(
        Employee.connection, sort=Employee.sort_argument()
    )
    # Allows sorting over multiple columns, by default over the primary key
    all_roles = SortableSQLAlchemyConnectionField(Role.connection)
    # Disable sorting over this field
    all_departments = SortableSQLAlchemyConnectionField(Department.connection, sort=None)


schema = graphene.Schema(query=Query)
