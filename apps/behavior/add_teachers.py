import csv
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from werkzeug.security import generate_password_hash

# Setup SQLAlchemy
Base = declarative_base()
engine = create_engine('sqlite:////home/behavioralreef/mysite/teachers.db', echo=False)
Session = sessionmaker(bind=engine)

class Teacher(Base):
    __tablename__ = 'teachers'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    password = Column(String(255), nullable=False)

    def __repr__(self):
        return f"<Teacher {self.name}>"

# Create database and tables
Base.metadata.create_all(engine)

# Add teachers from CSV
session = Session()
try:
    with open('/home/behavioralreef/teacher_data.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Check for existing teacher by email
            if session.query(Teacher).filter_by(email=row['email']).first():
                print(f"Skipping {row['email']} - already exists")
                continue
            # Add new teacher with hashed password
            teacher = Teacher(
                name=row['name'].strip(),
                email=row['email'].strip(),
                password=generate_password_hash(row['password'], method='pbkdf2:sha256')
            )
            session.add(teacher)
            print(f"Added {teacher.name}")
        session.commit()
        print("Done")
except FileNotFoundError:
    print("Error: teachers.csv not found")
    session.rollback()
except KeyError as e:
    print(f"Error: Missing column {e}")
    session.rollback()
except Exception as e:
    print(f"Error: {e}")
    session.rollback()
finally:
    session.close()