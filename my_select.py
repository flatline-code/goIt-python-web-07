from sqlalchemy import func, desc, select, and_

from src.models import Student, Teacher, Discipline, Grade, Group
from src.db import session
import pprint


def select_01():
    result = session.query(Student.fullname, 
                func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                .select_from(Grade)\
                .join(Student)\
                .group_by(Student.id)\
                .order_by(desc('avg_grade'))\
                .limit(5).all()
    return result

def select_02(discipline_id: int):
    result = session.query(Discipline.name, 
                  Student.fullname,
                  func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                  .select_from(Grade)\
                  .join(Student)\
                  .join(Discipline)\
                  .filter(Discipline.id == discipline_id)\
                  .group_by(Student.id, Discipline.name)\
                  .order_by(desc('avg_grade'))\
                  .limit(1).all()
    return result

def select_03(discipline_id: int):
    result = session.query(Discipline.name, 
                  Group.name,
                  func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                  .select_from(Grade)\
                  .join(Student)\
                  .join(Discipline)\
                  .join(Group)\
                  .filter(Discipline.id == discipline_id)\
                  .group_by(Group.name, Discipline.name)\
                  .order_by(desc('avg_grade'))\
                  .all()
    return result

def select_04():
    result = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade_from_all_grades'))\
                  .select_from(Grade).all()
    return result

def select_05(teacher_fullname: str):
    result = session.query(Teacher.fullname, Discipline.name)\
                  .select_from(Discipline)\
                  .join(Teacher)\
                  .filter(Teacher.fullname == teacher_fullname)\
                  .all()
    return result

def select_06(group_id: int):
    result = session.query(Group.name, 
                  Student.fullname)\
                  .select_from(Student)\
                  .join(Group)\
                  .filter(Group.id == group_id)\
                  .all()
    return result

def select_07(discipline_id: int):
    result = session.query(Discipline.name, Group.name, Student.fullname, Grade.grade)\
                  .select_from(Grade)\
                  .join(Student)\
                  .join(Discipline)\
                  .join(Group)\
                  .filter(Discipline.id == discipline_id)\
                  .order_by(Group.name)\
                  .all()
    return result

def select_08(teacher_id: int):
    result = session.query(Teacher.fullname, 
                  Discipline.name,
                  func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                  .select_from(Grade)\
                  .join(Discipline)\
                  .join(Teacher)\
                  .filter(Teacher.id == teacher_id)\
                  .group_by(Discipline.id, Teacher.fullname)\
                  .all()
    return result

def select_09(student_fullname: str):
    result = session.query(Student.fullname, 
                  Discipline.name)\
                  .select_from(Student)\
                  .join(Grade)\
                  .join(Discipline)\
                  .filter(Student.fullname == student_fullname)\
                  .group_by(Discipline.id, Student.fullname)\
                  .all()
    return result

def select_10(discipline_id, group_id):
    subquery = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == discipline_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1).scalar_subquery())

    r = session.query(Discipline.name,
                      Student.fullname,
                      Group.name,
                      Grade.date_of,
                      Grade.grade
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group)\
        .filter(and_(Discipline.id == discipline_id, Group.id == group_id, Grade.date_of == subquery)) \
        .order_by(desc(Grade.date_of)) \
        .all()
    return r

if __name__ == '__main__':
    pprint.pprint(select_01())
    pprint.pprint(select_02(2))
    pprint.pprint(select_03(1))
    pprint.pprint(select_04())
    pprint.pprint(select_05('Michael Shelton'))
    pprint.pprint(select_06(1))
    pprint.pprint(select_08(1))
    pprint.pprint(select_09('Wanda Carlson'))
    pprint.pprint(select_10(1, 1))
