from random import (random,randrange)

class RecordGeneratorScalaUtils:
    def __init__(self,lang):
        fnames = ["John", "Kevin", "Lydia", "Nathan", "Jane", "Liz", "Sam", "Ruby", "Peter", "Rob",
                   "Mahendra", "Noah", "Noemie", "Fred", "Anupam", "Stephanie", "Ken", "Sam",
                   "Jean-Georges", "Holden", "Murthy", "Jonathan", "Jean", "Georges", "Oliver"]
        lnames = ["Smith", "Mills", "Perrin", "Foster", "Kumar", "Jones", "Tutt", "Main",
                   "Haque", "Christie", "Karau", "Kahn", "Hahn", "Sanders"]
        articles = ["The", "My", "A", "Your", "Their"]
        adjectives = ["", "Great", "Beautiful", "Better", "Worse", "Gorgeous",
                       "Terrific", "Terrible", "Natural", "Wild"]
        nouns = ["Life", "Trip", "Experience", "Work", "Job", "Beach"]
        self.lang = lang
        daysInMonth =[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    def getLang(self):
        size = len(self.lang)
        return self.lang[randrange(1,size)]

    def getRandomInt(self,i):
        (random() * i)

    def getRating(self):
        self.getRandomInt(3) + 3


if __name__ == '__main__':
    lang = ["fr", "en", "es", "de", "it", "pt"]
    utils = RecordGeneratorScalaUtils(lang)
    print(utils.getLang())
    print(utils.getRandomInt(1))
    print(utils.getRating())