import sqlalchemy as sa

def get_mssql_engine():
    engine = sa.create_engine(
        "mssql+pyodbc://sa2:wwoor8862@localhost/master?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return engine
