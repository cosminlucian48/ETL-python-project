from lead.database_connections.MongoConnection import MongoConnection
from lead.database_connections.PostgresConnection import PostgresConnection


class DatabaseConnectionFactory:
    __connections = {}

    @staticmethod
    def get_connection(db_type):
        if db_type not in DatabaseConnectionFactory.__connections:
            if db_type == "postgres":
                DatabaseConnectionFactory.__connections[db_type] = PostgresConnection()
            elif db_type == "mongo":
                DatabaseConnectionFactory.__connections[db_type] = MongoConnection()
            # Add other database connection types as needed
        return DatabaseConnectionFactory.__connections[db_type]
