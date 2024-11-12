import pandas as pd
import logging
import re
from bilstein_slexa import global_vars

# Configure logging
logger = logging.getLogger("<Bilstein SLExA ETL>")


class GradeChecker:
    def __init__(self, db_connection):
        self.db = db_connection
        self.grade_list = self.get_grades_from_db()

    def get_grades_from_db(self):
        """Fetch grade names from the database and return as a list."""
        query = "SELECT name FROM grade WHERE active = TRUE"
        result = self.db.query(query)
        # Extract grade names from the query result
        return [row[0].strip() for row in result]

    def normalize_grade(self, grade):
        """Normalize the grade by converting to lowercase and removing spaces."""
        return grade.lower().replace(" ", "")

    def try_combinations(self, candidate):
        """
        Generate different combinations of the candidate to match against database references.

        Args:
            candidate (str): The grade candidate to match.

        Returns:
            Tuple[str, bool]: (Matched grade, True) if found, else (original candidate, False).
        """
        normalized_candidate = self.normalize_grade(candidate)

        # Generate possible combinations
        parts = re.split(r"\s+", candidate)  # Split on whitespace
        combinations = []

        # Generate combinations
        for i in range(1, len(parts) + 1):
            accumulated = "".join(parts[:i])  # Join first i parts as one segment
            remaining = (
                "".join(parts[i:]) if i < len(parts) else ""
            )  # Join remaining parts
            combinations.append(accumulated)
            if remaining:
                combinations.append(
                    remaining
                )  # Concatenate accumulated and remaining with space

        # Attempt to match each combination against reference grades
        for combo in combinations:
            normalized_combo = self.normalize_grade(combo)
            for reference in self.grade_list:
                if normalized_combo == self.normalize_grade(reference):
                    return reference, True

        return candidate, False  # Return original if no match found

    def match_grade(self, candidate):
        """Check if a candidate grade matches a grade in the database."""
        normalized_candidate = self.normalize_grade(candidate)

        # Direct match check
        for reference in self.grade_list:
            if normalized_candidate == self.normalize_grade(reference):
                return reference, True

        # Attempt to split and match largest segment
        if "+" in candidate or "-" in candidate:
            split_candidate = (
                candidate.split("+")[0] if "+" in candidate else candidate.split("-")[0]
            )
            normalized_split = self.normalize_grade(split_candidate)

            for reference in self.grade_list:
                if normalized_split == self.normalize_grade(reference):
                    # suffix = candidate[len(split_candidate):].strip()
                    return reference, True

        reference, matched_flag = self.try_combinations(candidate)
        if matched_flag:
            return reference, True

        return candidate, False  # Return original if no match

    def check_and_update_grade(self, df, grade_column="grade"):
        """Check and update grades in a DataFrame based on database reference."""
        for idx, candidate in df[grade_column].items():
            if isinstance(candidate, str):
                updated_grade, matched = self.match_grade(candidate)
                if matched:
                    logger.info(
                        f"Grade '{candidate}' matched with database entry. Updated to '{updated_grade}'"
                    )
                else:
                    message = f"Grade '{candidate}' with Bundle Id {df['bundle_id'].loc[idx]} was not found in database. No mapping applied."
                    global_vars["error_list"].append(message)
                    logger.warning(message)

                # Update the DataFrame with the validated or original grade
                df.at[idx, grade_column] = updated_grade
            else:
                message = f"Grade '{candidate}' with Bundle Id {df['bundle_id'].loc[idx]} is empty"
                global_vars["error_list"].append(message)
                logger.warning(message)

        return df
