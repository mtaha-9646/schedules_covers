# fix_and_show_db.py
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# --- Connect to SQLite ---
DB_PATH = "your_database.db"  # replace with your database file
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

# ---------------- Teachers ----------------
class Teacher(Base):
    __tablename__ = 'teachers'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), unique=True, nullable=False, index=True)
    password = Column(String(255), nullable=False)

# ---------------- Behaviour incidents ----------------
class Incident(Base):
    __tablename__ = 'incidents'
    id = Column(Integer, primary_key=True)
    esis = Column(String(50), nullable=False, index=True)
    name = Column(String(100), nullable=False)
    homeroom = Column(String(50), nullable=False)
    date_of_incident = Column(DateTime, default=datetime.utcnow)
    place_of_incident = Column(String(200), nullable=False)
    incident_grade = Column(String(50), nullable=False)
    action_taken = Column(String(200), nullable=False)
    incident_description = Column(Text, nullable=False)
    attachment = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    teacher_id = Column(Integer, nullable=False)

# ---------------- Students ----------------
class Students(Base):
    __tablename__ = 'students'
    id = Column(Integer, primary_key=True)
    esis = Column(String(50), nullable=False, unique=True, index=True)
    name = Column(String(150), nullable=False)
    homeroom = Column(String(50), nullable=True)

# --- Create all missing tables ---

# --- Function to show all tables ---
def show_all_data():
    for table_class in [Teacher, Incident, Students]:
        print(f"\n--- Table: {table_class.__tablename__} ---")
        rows = session.query(table_class).all()
        if not rows:
            print("No data")
            continue
        # Print column headers
        cols = table_class.__table__.columns.keys()
        print(" | ".join(cols))
        # Print rows
        for row in rows:
            print(" | ".join(str(getattr(row, col)) for col in cols))

if __name__ == "__main__":
    show_all_data()
