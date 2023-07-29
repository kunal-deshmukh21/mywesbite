from sqlalchemy import create_engine, text, insert, MetaData,Table, Column, Integer, VARCHAR
import os 

db_connection_string = os.environ.get('DB_CONNECTION_STRING')

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
    result = conn.execute(text("SELECT * FROM jobs"))
    jobs = []
    for row in result.all():
        jobs.append(dict(row._mapping))
    return jobs

def load_job_from_db(id):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM jobs WHERE id = :val"), {"val": id})
        rows = result.fetchone()
        if rows is None:
            return None
        else:
            return dict(rows._mapping)


metadata = MetaData()

applications_table = Table(
    'applications',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('job_id', Integer, nullable=False),
    Column('full_name', VARCHAR(40), nullable=False),
    Column('email', VARCHAR(40), nullable=False),
    Column('linkedin_url', VARCHAR(250), nullable=True),
    Column('education', VARCHAR(500), nullable=True),
    Column('work_experience', VARCHAR(500), nullable=True),
    Column('resume_url', VARCHAR(250), nullable=True),
)



def add_application_to_db(job_id, data):
    with engine.connect() as conn:
        query = insert(applications_table).values(
            job_id=job_id,
            full_name=data['full_name'],
            email=data['email'],
            linkedin_url=data['linkedin_url'],
            education=data['education'],
            work_experience=data['work_experience'],
            resume_url=data['resume_url']
        )

        conn.execute(query)
