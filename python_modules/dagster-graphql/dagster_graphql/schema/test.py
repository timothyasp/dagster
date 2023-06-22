import asyncio

import graphene


class GrapheneTestFields(graphene.ObjectType):
    class Meta:
        name = "TestFields"

    alwaysException = graphene.String()
    asyncString = graphene.String()

    def resolve_alwaysException(self, _):
        raise Exception("as advertised")

    async def resolve_asyncString(self, _):
        await asyncio.sleep(0)
        return "slept"
