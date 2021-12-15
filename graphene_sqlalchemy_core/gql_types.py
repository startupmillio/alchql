import graphene
from graphene.types import scalars


def _construct_type(type_: type):
    def init_(self, *args, model_field=None, **kwargs):
        super(type_, self).__init__(*args, **kwargs)
        self.model_field = model_field

    return type(type_.__name__, (type_,), {"__init__": init_})


ID = _construct_type(scalars.ID)
Int = _construct_type(scalars.Int)
BigInt = _construct_type(scalars.BigInt)
Float = _construct_type(scalars.Float)
String = _construct_type(scalars.String)
Boolean = _construct_type(scalars.Boolean)

List = _construct_type(graphene.List)
NonNull = _construct_type(graphene.NonNull)
Date = _construct_type(graphene.Date)
