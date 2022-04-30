class Scorer:
    def __init__(self, ground_truth):
        self.__ground_truth = ground_truth
        
    def GetIntersectionScore(self, res):
        answers = {}

        for v in self.__ground_truth:
            answers[v[0]] = False

        for v in res:
            if v[0] in answers:
                answers[v[0]] = True

        misses = 0
        for k, v in answers.items():
            if not v:
                misses += 1

        return 1.0 - misses / len(answers)
