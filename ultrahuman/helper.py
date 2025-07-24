from sensorfabric.mdh import MDH
from datetime import datetime, timezone
import math

class ParticipantNotEnrolled(Exception):
    """Raised when the participant is not enrolled in the study."""
    pass

class Helper:
    """ Helper class for reporting template"""
    def __init__(self, mdh: MDH, participant_id: str):
        """
        Paramters
        ---------
        1. mdh (sensorfabric.mdh.MDH) - A sensorfabric MDH object.
        2. participant_id (string) - Participant ID for which the report is being
           created.

        Returns
        -------
        Helper object

        Exceptions
        -----
        ParticipantNotEnrolled - If the participant status is not enrolled
        """
        self.mdh = mdh
        self.participant_id = participant_id

        # Go ahead and get all the information for the participant from MDH
        self.participant = mdh.getParticipant(participant_id)

        # Make sure that this participant has enrolled
        if not self.participant['enrolled']:
            raise ParticipantNotEnrolled('Participant is not yet enrolled in the study')

    def getParticipant(self) -> dict:
        """Returns the MDH participant dictionary"""
        return self.participant

    def weeksEnrolled(self) -> int:
        """
        Returns the total number of weeks (rounded up) the
        participant has enrolled in the study.
        """
        enrolled_on = datetime.fromisoformat(self.participant['enrollmentDate'])
        today = datetime.now(timezone.utc)
        delta = today - enrolled_on

        weeks = math.ceil(delta.days / 7)

        return weeks
