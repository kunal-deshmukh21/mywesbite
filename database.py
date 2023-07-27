from sqlalchemy import create_engine, text
import os 

db_connection_string = os.environ.get('DB_CONNECTION_STRING')

if not db_connection_string:
    raise ValueError("DB_CONNECTION_STRING environment variable is not set.")


engine = create_engine(
    db_connection_string,
    connect_args={
        "ssl": {
            "ssl_ca": "/etc/ssl/cert.pem" 
        }
    }
    )

def load_jobs_from_db():
  with engine.connect() as conn:
    result = conn.execute(text("select * from jobs"))
    jobs = []
    for row in result.all():
        jobs.append(dict(row._mapping))
    return jobs
