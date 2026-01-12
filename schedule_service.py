from __future__ import annotations
import re
from datetime import date, datetime
from typing import Any, Callable

import pandas as pd

from models import DutyAssignment, ScheduleEntry, TeacherManifest

DAY_ORDER = ["Mo", "Tu", "We", "Th", "Fr"]
DAY_LABELS = {
    "Mo": "Monday",
    "Tu": "Tuesday",
    "We": "Wednesday",
    "Th": "Thursday",
    "Fr": "Friday",
}
DAY_INDEX = {code: idx for idx, code in enumerate(DAY_ORDER)}
WEEKDAY_TO_DAY_CODE = {idx: code for idx, code in enumerate(DAY_ORDER)}
DAY_LABEL_TO_CODE = {label: code for code, label in DAY_LABELS.items()}
NON_CLASS_DETAILS = {"homeroom"}

PERIOD_CANONICAL = {
    "Homeroom 7:30 - 7:45": "Homeroom",
    "P1 7:30 - 8:20": "P1",
    "Period 1 7:50 - 8:45": "P1",
    "P2 8:25 - 9:15": "P2",
    "Period 2 8:50 - 9:45": "P2",
    "P3 10:10 - 11:00": "P3",
    "Period 3 - G6 9:50 - 10:45": "P3",
    "Period 3 - G7 10:00 - 10:55": "P3",
    "P4 - G6 11:45 - 12:40": "P4",
    "P4 - G7 11:00 - 11:55": "P4",
    "P4 11:05 - 11:55": "P4",
    "P5 12:00 - 12:50": "P5",
    "Period 5 12:55 - 1:45": "P5",
    "P6 1:00 - 1:50": "P6",
    "Period 6 1:50 - 2:45": "P6",
    "P7 1:55 - 2:45": "P7",
}

ORDERED_PERIODS = [
    "Homeroom",
    "P1",
    "P2",
    "P3",
    "P4",
    "P5",
    "P6",
    "P7",
]

GRADE_PATTERN = re.compile(r"(?:G)?(6|7|10|11|12)")


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower())
    return slug.strip("-")


class ScheduleManager:
    def __init__(self, excel_path: str, session_factory: Callable | None = None):
        self.excel_path = excel_path
        self._session_factory = session_factory
        self._df = self._load_schedule()
        if self._df.empty and self._session_factory:
            self.import_from_excel()
            self._df = self._load_schedule()
        self._dynamic_rows = pd.DataFrame(columns=self._df.columns)
        self._course_count_column = self._select_course_count_column()
        self._manifest = self._load_teacher_manifest()
        self._teachers = self._build_teacher_index()
        self._name_index = self._build_name_index()
        self._email_index = self._build_email_index()

    def reload_data(self) -> None:
        """Reload the schedule data from disk, rebuilding teacher metadata."""
        self._df = self._load_schedule()
        self._dynamic_rows = pd.DataFrame(columns=self._df.columns)
        self._course_count_column = self._select_course_count_column()
        self._manifest = self._load_teacher_manifest()
        self._teachers = self._build_teacher_index()
        self._name_index = self._build_name_index()
        self._email_index = self._build_email_index()

    def export_to_excel(self, excel_path: str | None = None) -> int:
        if not self._session_factory:
            return 0
        with self._session_factory() as session:
            rows = session.query(ScheduleEntry).all()
        if not rows:
            return 0
        data = [
            {
                "Teacher": row.teacher,
                "Day": row.day,
                "Period": row.period,
                "Details": row.details,
                "email": row.email,
                "subject": row.subject,
                "course_count": row.course_count,
            }
            for row in rows
        ]
        df = pd.DataFrame(data)
        output_path = excel_path or self.excel_path
        df.to_excel(output_path, index=False)
        return len(rows)

    def get_entries_for_teacher(self, slug: str) -> list[dict[str, Any]]:
        if not self._session_factory:
            return []
        meta = self.get_teacher(slug)
        if not meta:
            return []
        with self._session_factory() as session:
            rows = (
                session.query(ScheduleEntry)
                .filter(ScheduleEntry.teacher == meta["name"])
                .order_by(ScheduleEntry.day_code, ScheduleEntry.period_rank)
                .all()
            )
        return [
            {
                "id": row.id,
                "day_code": row.day_code,
                "day": row.day,
                "period": row.period,
                "period_raw": row.period_raw,
                "details": row.details,
                "subject": row.subject,
            }
            for row in rows
        ]

    def update_teacher_info(
        self,
        slug: str,
        name: str,
        email: str | None,
        subject: str | None,
        course_count: int | None,
    ) -> str | None:
        if not self._session_factory:
            return None
        meta = self.get_teacher(slug)
        if not meta:
            return None
        teacher_name = meta["name"]
        new_name = name.strip() if name else teacher_name
        new_email = email.strip() if email else meta.get("email")
        new_subject = subject.strip() if subject else meta.get("subject")
        new_course_count = course_count if course_count is not None else meta.get("course_total")
        with self._session_factory() as session:
            session.query(ScheduleEntry).filter(
                ScheduleEntry.teacher == teacher_name
            ).update(
                {
                    ScheduleEntry.teacher: new_name,
                    ScheduleEntry.email: new_email,
                    ScheduleEntry.subject: new_subject,
                    ScheduleEntry.course_count: new_course_count,
                }
            )
            session.commit()
        self.reload_data()
        return slugify(new_name)

    def update_schedule_entry(
        self,
        entry_id: int,
        day_code: str,
        period_label: str,
        period_raw: str | None,
        details: str,
        subject: str | None,
    ) -> bool:
        if not self._session_factory:
            return False
        normalized_day = self.normalize_day(day_code)
        if not normalized_day or not period_label:
            return False
        with self._session_factory() as session:
            record = session.get(ScheduleEntry, entry_id)
            if not record:
                return False
            record.day_code = normalized_day
            record.day = DAY_LABELS.get(normalized_day, normalized_day)
            record.period = period_label.strip()
            raw_value = period_raw.strip() if period_raw else record.period
            record.period_raw = raw_value
            record.period_group = self._normalize_period(raw_value) or raw_value
            record.period_rank = (
                self._period_rank(record.period_group or raw_value) or len(ORDERED_PERIODS)
            )
            record.details = details.strip() if details else ""
            record.details_display = record.details or "General Duty"
            record.grade_detected = self._detect_grade(record.details or "")
            if subject is not None:
                record.subject = subject.strip()
            session.commit()
        self.reload_data()
        return True

    def add_schedule_entry(
        self,
        slug: str,
        day_code: str,
        period_label: str,
        period_raw: str | None,
        details: str,
        subject: str | None,
    ) -> bool:
        if not self._session_factory:
            return False
        meta = self.get_teacher(slug)
        if not meta:
            return False
        normalized_day = self.normalize_day(day_code)
        if not normalized_day or not period_label:
            return False
        raw_value = period_raw.strip() if period_raw else period_label.strip()
        period_group = self._normalize_period(raw_value) or raw_value
        record = ScheduleEntry(
            teacher=meta["name"],
            day=DAY_LABELS.get(normalized_day, normalized_day),
            day_code=normalized_day,
            period=period_label.strip(),
            period_raw=raw_value,
            period_group=period_group,
            period_rank=self._period_rank(period_group) or len(ORDERED_PERIODS),
            details=details.strip() if details else "",
            details_display=details.strip() if details else "General Duty",
            grade_detected=self._detect_grade(details or ""),
            email=meta.get("email"),
            subject=(subject.strip() if subject else meta.get("subject")),
            course_count=self._as_int(meta.get("course_total")),
        )
        with self._session_factory() as session:
            session.add(record)
            session.commit()
        self.reload_data()
        return True

    def delete_schedule_entry(self, entry_id: int) -> bool:
        if not self._session_factory:
            return False
        with self._session_factory() as session:
            record = session.get(ScheduleEntry, entry_id)
            if not record:
                return False
            session.delete(record)
            session.commit()
        self.reload_data()
        return True

    def _combined_schedule_df(self) -> pd.DataFrame:
        if self._dynamic_rows.empty:
            return self._df
        return pd.concat([self._df, self._dynamic_rows], ignore_index=True)

    def clear_cover_assignments(self) -> None:
        self._dynamic_rows = pd.DataFrame(columns=self._df.columns)

    def rebuild_cover_assignments(
        self, assignments: dict[str, list[dict[str, Any]]]
    ) -> None:
        self.clear_cover_assignments()
        for rows in assignments.values():
            for entry in rows:
                self._append_cover_row(entry)

    def _append_cover_row(self, assignment: dict[str, Any]) -> None:
        if not assignment:
            return
        teacher_name = assignment.get("cover_teacher")
        if not teacher_name:
            return
        day_code = self._day_code_for_assignment(assignment)
        period_label = str(assignment.get("period_label") or assignment.get("period_raw") or "Cover").strip()
        period_raw = str(assignment.get("period_raw") or assignment.get("period_label") or period_label).strip()
        period_group = self._normalize_period(period_label) or self._normalize_period(period_raw) or period_label
        period_rank = self._period_rank(period_group or period_raw) or len(ORDERED_PERIODS)
        details = str(assignment.get("class_details") or assignment.get("class_subject") or "Cover duty").strip()
        grade_value = assignment.get("class_grade") or ""
        grade_detected = self._detect_grade(str(grade_value)) or self._detect_grade(details)
        cover_slug = assignment.get("cover_slug")
        teacher_meta = self.get_teacher(cover_slug) if cover_slug else None
        course_total = teacher_meta.get("course_total") if teacher_meta else 0
        email = assignment.get("cover_email") or (teacher_meta.get("email") if teacher_meta else None)
        day_label = DAY_LABELS.get(day_code) if day_code else assignment.get("day_label") or "Cover"
        class_subject = assignment.get("class_subject")
        subject_value = class_subject or (teacher_meta.get("subject") if teacher_meta else None) or "General"
        row = {
            "Teacher": teacher_name,
            "Day": day_label,
            "Period": period_label,
            "Details": details,
            "course_count": course_total or 0,
            "email": email or "schedule@charterschools.ae",
            "subject": subject_value,
            "DayCode": day_code or "",
            "PeriodRaw": period_raw,
            "PeriodGroup": period_group,
            "PeriodRank": period_rank,
            "GradeDetected": grade_detected,
            "DetailsDisplay": details,
        }
        self._dynamic_rows = pd.concat(
            [self._dynamic_rows, pd.DataFrame([row])],
            ignore_index=True,
        )

    def _day_code_for_assignment(self, assignment: dict[str, Any]) -> str | None:
        label = assignment.get("day_label")
        if label:
            normalized = label.strip()
            code = DAY_LABEL_TO_CODE.get(normalized) or DAY_LABEL_TO_CODE.get(normalized.title())
            if code:
                return code
        date_value = assignment.get("date")
        if date_value:
            try:
                parsed = datetime.fromisoformat(date_value)
                return WEEKDAY_TO_DAY_CODE.get(parsed.weekday())
            except ValueError:
                pass
        return None

    def _load_schedule(self) -> pd.DataFrame:
        if not self._session_factory:
            return self._load_schedule_from_excel()
        with self._session_factory() as session:
            rows = session.query(ScheduleEntry).all()
        if not rows:
            return pd.DataFrame(
                columns=[
                    "Teacher",
                    "Day",
                    "DayCode",
                    "Period",
                    "PeriodRaw",
                    "PeriodGroup",
                    "PeriodRank",
                    "GradeDetected",
                    "Details",
                    "DetailsDisplay",
                    "email",
                    "subject",
                    "course_count",
                ]
            )
        data = [
            {
                "Teacher": row.teacher,
                "Day": row.day,
                "DayCode": row.day_code,
                "Period": row.period,
                "PeriodRaw": row.period_raw,
                "PeriodGroup": row.period_group,
                "PeriodRank": row.period_rank,
                "GradeDetected": row.grade_detected,
                "Details": row.details,
                "DetailsDisplay": row.details_display,
                "email": row.email,
                "subject": row.subject,
                "course_count": row.course_count,
            }
            for row in rows
        ]
        return pd.DataFrame(data)

    def _load_schedule_from_excel(self) -> pd.DataFrame:
        df = pd.read_excel(self.excel_path)
        df = df.dropna(subset=["Teacher"])
        df["DayCode"] = df["Day"].str.strip().fillna("")
        df["PeriodRaw"] = df["Period"].str.strip().fillna("")
        df["PeriodGroup"] = df["PeriodRaw"].map(self._normalize_period)
        df["PeriodGroup"] = df["PeriodGroup"].fillna(df["PeriodRaw"])
        df["PeriodRank"] = df["PeriodGroup"].map(self._period_rank).fillna(len(ORDERED_PERIODS))
        df["GradeDetected"] = df["Details"].fillna("").apply(self._detect_grade)
        df["DetailsDisplay"] = df["Details"].fillna("General Duty")
        return df

    def import_from_excel(self) -> int:
        if not self._session_factory:
            return 0
        df = self._load_schedule_from_excel()
        manifest = self._load_teacher_manifest_from_excel()
        entries = []
        for _, row in df.iterrows():
            entries.append(
                ScheduleEntry(
                    teacher=str(row.get("Teacher") or "").strip(),
                    day=str(row.get("Day") or "").strip(),
                    day_code=str(row.get("DayCode") or "").strip(),
                    period=str(row.get("Period") or "").strip(),
                    period_raw=str(row.get("PeriodRaw") or "").strip(),
                    period_group=str(row.get("PeriodGroup") or "").strip(),
                    period_rank=self._as_int(row.get("PeriodRank")),
                    grade_detected=self._as_int(row.get("GradeDetected")),
                    details=str(row.get("Details") or "").strip(),
                    details_display=str(row.get("DetailsDisplay") or "").strip(),
                    email=str(row.get("email") or "").strip(),
                    subject=str(row.get("subject") or "").strip(),
                    course_count=self._as_int(row.get("course_count")),
                )
            )
        with self._session_factory() as session:
            session.query(ScheduleEntry).delete()
            session.query(TeacherManifest).delete()
            if entries:
                session.bulk_save_objects(entries)
            if manifest:
                session.bulk_save_objects(
                    [
                        TeacherManifest(
                            slug=slug,
                            name=data.get("name") or slug,
                            email=data.get("email"),
                        )
                        for slug, data in manifest.items()
                    ]
                )
            session.commit()
        return len(entries)

    def _select_course_count_column(self) -> str | None:
        candidates = {"course_count", "course count", "number of course", "number_of_course"}
        for column in self._df.columns:
            if str(column).strip().lower() in candidates:
                return column
        return None

    def _normalize_period(self, period: str) -> str | None:
        if not period:
            return None
        normalized = PERIOD_CANONICAL.get(period.strip())
        if normalized:
            return normalized
        lowered = period.lower()
        for alias, canonical in PERIOD_CANONICAL.items():
            if alias.lower() == lowered:
                return canonical
        if lowered.startswith("p"):
            digit = ""
            for char in lowered[1:]:
                if char.isdigit():
                    digit += char
                elif digit:
                    break
            if digit:
                return f"P{digit}"
        return period

    def _period_rank(self, period: str) -> int | None:
        try:
            return ORDERED_PERIODS.index(period)
        except ValueError:
            return None

    def _detect_grade(self, details: str) -> int | None:
        match = GRADE_PATTERN.search(details)
        if not match:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None

    def _build_teacher_index(self) -> dict[str, dict]:
        teachers = {}
        for teacher, group in self._df.groupby("Teacher"):
            slug = slugify(teacher)
            grade_levels = self._ordered_grade_levels(group)
            level_label = self._grade_label(grade_levels)
            course_count = self._course_count_for_group(group)
            email = (
                group["email"].dropna().iloc[0]
                if not group["email"].dropna().empty
                else "schedule@charterschools.ae"
            )
            subject = (
                group["subject"].dropna().iloc[0] if not group["subject"].dropna().empty else "General"
            )
            primary_class = self._primary_class_label(group)
            teachers[slug] = {
                "name": teacher,
                "slug": slug,
                "email": email,
                "subject": subject,
                "course_total": course_count,
                "grade_levels": grade_levels,
                "level_label": level_label,
                "primary_class": primary_class,
                "day_count": group["Day"].nunique(),
            }
        for slug, manifest_data in (self._manifest or {}).items():
            if slug in teachers:
                if manifest_data.get("email") and not teachers[slug].get("email"):
                    teachers[slug]["email"] = manifest_data["email"]
                continue
            teachers[slug] = {
                "name": manifest_data["name"],
                "slug": slug,
                "email": manifest_data.get("email") or "schedule@charterschools.ae",
                "subject": "General",
                "course_total": 0,
                "grade_levels": [],
                "level_label": "General",
                "primary_class": "No schedule yet",
                "day_count": 0,
            }
        return teachers

    def _build_name_index(self) -> dict[str, dict]:
        return {meta["name"]: meta for meta in self._teachers.values()}

    def _build_email_index(self) -> dict[str, dict]:
        index = {}
        for meta in self._teachers.values():
            email = meta.get("email")
            if not email:
                continue
            index[email.strip().lower()] = meta
        return index

    def find_teacher_by_email(self, email: str) -> dict | None:
        if not email:
            return None
        return self._email_index.get(email.strip().lower())

    def _course_count_for_group(self, group: pd.DataFrame) -> int:
        if not self._course_count_column:
            return 0
        values = group[self._course_count_column].dropna()
        if values.empty:
            return 0
        return self._as_int(values.iloc[0])

    def _grade_label(self, grade_levels: list[int]) -> str:
        if not grade_levels:
            return "General"
        middle = {6, 7}
        high = {10, 11, 12}
        unique_grades = set(grade_levels)
        has_middle = bool(unique_grades & middle)
        has_high = bool(unique_grades & high)
        if has_middle and has_high:
            return "Middle & High School"
        primary_grade = grade_levels[0]
        if primary_grade in middle:
            return "Middle School"
        if primary_grade in high:
            return "High School"
        if has_high:
            return "High School"
        if has_middle:
            return "Middle School"
        return "General"

    def _ordered_group(self, group: pd.DataFrame) -> pd.DataFrame:
        return (
            group.assign(
                DayRank=group["DayCode"].map(
                    lambda code: DAY_INDEX.get(code, len(DAY_ORDER))
                ),
                PeriodRankSortable=group["PeriodRank"].fillna(len(ORDERED_PERIODS)),
            )
            .sort_values(["DayRank", "PeriodRankSortable"])
            .reset_index(drop=True)
        )

    def _ordered_grade_levels(self, group: pd.DataFrame) -> list[int]:
        ordered = self._ordered_group(group)
        seen: set[int] = set()
        grades: list[int] = []
        for _, row in ordered.iterrows():
            grade = row.get("GradeDetected")
            if not grade:
                continue
            try:
                grade_value = int(grade)
            except (TypeError, ValueError):
                continue
            if grade_value in seen:
                continue
            seen.add(grade_value)
            grades.append(grade_value)
        return grades

    def _primary_class_label(self, group: pd.DataFrame) -> str:
        fallback = None
        ordered = self._ordered_group(group)
        for _, row in ordered.iterrows():
            label = (row["DetailsDisplay"] or "").strip()
            if not label:
                continue
            if fallback is None:
                fallback = label
            if label.lower() not in NON_CLASS_DETAILS:
                return label
        return fallback or "General Duty"

    def _load_teacher_manifest(self) -> dict[str, dict] | None:
        if not self._session_factory:
            return self._load_teacher_manifest_from_excel()
        with self._session_factory() as session:
            records = session.query(TeacherManifest).all()
        if not records:
            return None
        return {record.slug: {"name": record.name, "email": record.email} for record in records}

    def _load_teacher_manifest_from_excel(self) -> dict[str, dict] | None:
        try:
            workbook = pd.ExcelFile(self.excel_path)
        except (ValueError, FileNotFoundError):
            return None
        for sheet_name in workbook.sheet_names:
            manifest = self._manifest_from_sheet(sheet_name)
            if manifest:
                return manifest
        return None

    def _manifest_from_sheet(self, sheet_name: str) -> dict[str, dict] | None:
        try:
            df = pd.read_excel(self.excel_path, sheet_name=sheet_name)
        except Exception:
            return None
        manifest = self._manifest_from_structured_df(df)
        if manifest:
            return manifest
        return self._manifest_from_simple_list(sheet_name)

    def _manifest_from_structured_df(self, df: pd.DataFrame) -> dict[str, dict] | None:
        columns = {
            str(col).strip().lower(): col for col in df.columns if isinstance(col, str)
        }
        if "name" not in columns:
            return None
        matched_col = columns.get("matched contact")
        manifest: dict[str, dict] = {}
        for _, row in df.iterrows():
            raw_name = row.get(columns["name"])
            if pd.isna(raw_name):
                continue
            raw_name = str(raw_name).strip()
            if not raw_name:
                continue
            contact_value = row.get(matched_col) if matched_col else None
            contact_name, contact_email = self._parse_manifest_contact(contact_value)
            canonical_name = contact_name or raw_name
            slug = slugify(canonical_name)
            manifest[slug] = {"name": canonical_name, "email": contact_email}
        return manifest or None

    def _manifest_from_simple_list(self, sheet_name: str) -> dict[str, dict] | None:
        try:
            df = pd.read_excel(self.excel_path, sheet_name=sheet_name, header=None)
        except Exception:
            return None
        df = df.dropna(how="all")
        if df.shape[1] < 2:
            return None
        manifest: dict[str, dict] = {}
        for _, row in df.iterrows():
            raw_email = row.iloc[0]
            raw_name = row.iloc[1]
            if pd.isna(raw_email) or pd.isna(raw_name):
                continue
            email = str(raw_email).strip()
            name = str(raw_name).strip()
            if "@" not in email or not name:
                continue
            slug = slugify(name)
            manifest[slug] = {"name": name, "email": email}
        return manifest or None

    def _parse_manifest_contact(self, raw_contact) -> tuple[str | None, str | None]:
        if raw_contact is None or (isinstance(raw_contact, float) and pd.isna(raw_contact)):
            return None, None
        text = str(raw_contact)
        name_match = re.search(r'"([^"]+)"', text)
        email_match = re.search(r"<([^>]+)>", text)
        return (name_match.group(1) if name_match else None, email_match.group(1) if email_match else None)

    @property
    def teacher_cards(self) -> list[dict]:
        cards = []
        for meta in self._teachers.values():
            cards.append(meta)
        return sorted(cards, key=lambda card: card["name"])

    @property
    def teacher_count(self) -> int:
        return len(self._teachers)

    @property
    def stats(self) -> dict:
        levels = {"Middle School": 0, "High School": 0, "Middle & High School": 0, "General": 0}
        for meta in self._teachers.values():
            levels[meta["level_label"]] = levels.get(meta["level_label"], 0) + 1
        return {
            "total_teachers": self.teacher_count,
            "middle": levels["Middle School"],
            "high": levels["High School"],
            "split": levels["Middle & High School"],
            "general": levels["General"],
        }

    def get_teacher(self, slug: str) -> dict | None:
        return self._teachers.get(slug)

    def get_schedule_for_teacher(self, slug: str) -> dict | None:
        meta = self.get_teacher(slug)
        if not meta:
            return None
        combined = self._combined_schedule_df()
        schedule_df = combined[combined["Teacher"] == meta["name"]]
        schedule_by_day = []
        for day_code in DAY_ORDER:
            day_name = DAY_LABELS.get(day_code, day_code)
            day_rows = schedule_df[schedule_df["DayCode"] == day_code]
            max_periods = self._max_periods_for_level(meta["level_label"], day_code)
            scheduled_count = len(day_rows[day_rows["PeriodGroup"] != "Homeroom"])
            day_sections = self._group_periods(day_rows)
            schedule_by_day.append(
                {
                    "code": day_code,
                    "label": day_name,
                    "sections": day_sections,
                    "scheduled_count": scheduled_count,
                    "max_periods": max_periods,
                    "free_periods": max(0, max_periods - scheduled_count),
                }
            )
        return {"meta": meta, "schedule": schedule_by_day}

    def day_summary_for_teacher(self, slug: str, day_code: str) -> dict:
        data = self.get_schedule_for_teacher(slug)
        if data:
            for day in data["schedule"]:
                if day.get("code") == day_code:
                    return day
        meta = self.get_teacher(slug)
        level_label = meta["level_label"] if meta else "General"
        max_periods = self._max_periods_for_level(level_label, day_code)
        return {
            "code": day_code,
            "label": DAY_LABELS.get(day_code, day_code),
            "sections": [],
            "scheduled_count": 0,
            "max_periods": max_periods,
            "free_periods": max_periods,
        }

    def all_teacher_schedules(self) -> list[dict]:
        schedules = []
        for slug in sorted(self._teachers.keys(), key=lambda slug: self._teachers[slug]["name"]):
            schedule = self.get_schedule_for_teacher(slug)
            if schedule:
                schedules.append(schedule)
        return schedules

    def _group_periods(self, day_rows: pd.DataFrame) -> list[dict]:
        sections = []
        for period in ORDERED_PERIODS:
            bucket = day_rows[day_rows["PeriodGroup"] == period]
            if bucket.empty:
                continue
            sections.append(self._section_for_bucket(period, bucket))
        overflow = day_rows[~day_rows["PeriodGroup"].isin(ORDERED_PERIODS)]
        if not overflow.empty:
            sections.append(self._section_for_bucket("Additional", overflow))
        return sections

    def _section_for_bucket(self, label: str, bucket: pd.DataFrame) -> dict:
        times = sorted(bucket["PeriodRaw"].unique())
        return {
            "period": label,
            "time": ", ".join(times),
            "details": [
                {
                    "details": row["DetailsDisplay"],
                    "subject": row["subject"],
                    "grade": f"G{row['GradeDetected']}" if row["GradeDetected"] else "G - N/A",
                    "period_raw": row["PeriodRaw"],
                }
                for _, row in bucket.sort_values("PeriodRank").iterrows()
            ],
        }

    @staticmethod
    def _parse_date_value(value: date | str | None) -> date | None:
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError:
                return None
        return None

    def _duty_records_for_slot(
        self,
        assignment_date: date | str | None,
        period_label: str,
    ) -> list[DutyAssignment]:
        if not self._session_factory or not assignment_date or not period_label:
            return []
        parsed_date = self._parse_date_value(assignment_date)
        if not parsed_date:
            return []
        period_group = self._normalize_period(period_label) or period_label
        with self._session_factory() as session:
            rows = (
                session.query(DutyAssignment)
                .filter(
                    DutyAssignment.assignment_date == parsed_date,
                    DutyAssignment.slot_type == "period",
                    DutyAssignment.period_label == period_group,
                )
                .all()
            )
        return rows

    def teachers_available(
        self,
        day_code: str,
        period_label: str,
        assignment_date: date | str | None = None,
    ) -> list[dict]:
        current = self._combined_schedule_df()
        scheduled = current[
            (current["DayCode"] == day_code) & (current["PeriodGroup"] == period_label)
        ]["Teacher"]
        scheduled_set = set(scheduled)
        duty_records = self._duty_records_for_slot(assignment_date, period_label)
        duty_emails = {
            record.teacher_email.strip().lower()
            for record in duty_records
            if record.teacher_email
        }
        duty_names = {
            record.teacher_name.strip().lower()
            for record in duty_records
            if record.teacher_name
        }
        available = []
        for slug, meta in self._teachers.items():
            if meta["name"] not in scheduled_set:
                email_key = (meta.get("email") or "").strip().lower()
                name_key = (meta.get("name") or "").strip().lower()
                if email_key and email_key in duty_emails:
                    continue
                if name_key and name_key in duty_names:
                    continue
                available.append(meta)
        return available

    def teachers_occupied(
        self,
        day_code: str,
        period_label: str,
        assignment_date: date | str | None = None,
    ) -> list[dict]:
        current = self._combined_schedule_df()
        scheduled = current[
            (current["DayCode"] == day_code) & (current["PeriodGroup"] == period_label)
        ]
        result = {}
        seen_keys: set[str] = set()
        for _, row in scheduled.iterrows():
            result[row["Teacher"]] = {
                "name": row["Teacher"],
                "period": row["PeriodGroup"],
                "details": row["DetailsDisplay"],
                "subject": row["subject"],
            }
            name_key = row["Teacher"].strip().lower()
            if name_key:
                seen_keys.add(name_key)
            meta = self._name_index.get(row["Teacher"])
            if meta and meta.get("email"):
                seen_keys.add(meta["email"].strip().lower())
        enriched = []
        for teacher_name, row_data in result.items():
            meta = self._name_index.get(teacher_name)
            enriched.append(
                {
                    **row_data,
                    "level_label": meta["level_label"] if meta else "General",
                    "grade_levels": meta.get("grade_levels", []) if meta else [],
                }
            )
        duty_records = self._duty_records_for_slot(assignment_date, period_label)
        for record in duty_records:
            email_key = (record.teacher_email or "").strip().lower()
            name_key = (record.teacher_name or "").strip().lower()
            if email_key and email_key in seen_keys:
                continue
            if name_key and name_key in seen_keys:
                continue
            if email_key:
                seen_keys.add(email_key)
            if name_key:
                seen_keys.add(name_key)
            meta = None
            if email_key:
                meta = self._email_index.get(email_key)
            if not meta and name_key:
                meta = self._name_index.get(record.teacher_name)
            display_name = record.teacher_name or (meta.get("name") if meta else None) or record.teacher_email or "Unknown"
            enriched.append(
                {
                    "name": display_name,
                    "period": period_label,
                    "details": record.label or "Duty assignment",
                    "subject": "Duty",
                    "level_label": meta["level_label"] if meta else "General",
                    "grade_levels": meta.get("grade_levels", []) if meta else [],
                }
            )
        return enriched

    def normalize_day(self, day: str) -> str | None:
        if not day:
            return None
        trimmed = day.strip().capitalize()
        for code, label in DAY_LABELS.items():
            if trimmed.lower() in {code.lower(), label.lower()}:
                return code
        return None

    def normalize_period(self, raw: str) -> str | None:
        if not raw:
            return None
        return self._normalize_period(raw.strip())

    @staticmethod
    def _as_int(value) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return 0

    def available_for_slot(
        self,
        day_code: str,
        period_label: str,
        assignment_date: date | str | None = None,
    ) -> dict:
        available = self.teachers_available(day_code, period_label, assignment_date=assignment_date)
        occupied = self.teachers_occupied(day_code, period_label, assignment_date=assignment_date)
        return {
            "available": available,
            "occupied": occupied,
            "period": period_label,
            "day": DAY_LABELS.get(day_code, day_code),
        }

    def available_for_slot_api(
        self,
        day_code: str,
        period_label: str,
        assignment_date: date | str | None = None,
    ) -> dict:
        available = self.teachers_available_for_api(
            day_code,
            period_label,
            assignment_date=assignment_date,
        )
        occupied = self.teachers_occupied(day_code, period_label, assignment_date=assignment_date)
        return {
            "available": available,
            "occupied": occupied,
            "period": period_label,
            "day": DAY_LABELS.get(day_code, day_code),
        }

    def teachers_available_for_api(
        self,
        day_code: str,
        period_label: str,
        assignment_date: date | str | None = None,
    ) -> list[dict]:
        available = self.teachers_available(day_code, period_label, assignment_date=assignment_date)
        period_group = self._normalize_period(period_label) or period_label
        if period_group not in ORDERED_PERIODS:
            return available
        index = ORDERED_PERIODS.index(period_group)
        before = ORDERED_PERIODS[max(0, index - 2) : index]
        after = ORDERED_PERIODS[index + 1 : index + 3]
        if len(before) < 2 and len(after) < 2:
            return available
        scheduled = self._scheduled_periods_by_teacher(day_code)
        filtered = []
        for teacher in available:
            name = teacher.get("name")
            periods = scheduled.get(name, set())
            before_blocked = len(before) == 2 and before[0] in periods and before[1] in periods
            after_blocked = len(after) == 2 and after[0] in periods and after[1] in periods
            if before_blocked or after_blocked:
                continue
            filtered.append(teacher)
        return filtered

    def _scheduled_periods_by_teacher(self, day_code: str) -> dict[str, set[str]]:
        current = self._combined_schedule_df()
        day_rows = current[current["DayCode"] == day_code]
        scheduled: dict[str, set[str]] = {}
        for _, row in day_rows.iterrows():
            teacher = row.get("Teacher")
            if not teacher:
                continue
            period = row.get("PeriodGroup") or row.get("PeriodRaw") or row.get("Period")
            if not period:
                continue
            scheduled.setdefault(teacher, set()).add(str(period))
        return scheduled

    def _max_periods_for_level(self, level_label: str, day_code: str) -> int:
        is_friday = day_code == "Fr"
        max_high = 5 if is_friday else 7
        max_middle = 3 if is_friday else 6
        if level_label in {"High School", "Middle & High School"}:
            return max_high
        if level_label == "Middle School":
            return max_middle
        return max_middle
