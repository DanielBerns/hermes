from pathlib import Path

def tester():
    print("__file__", __file__)
    print("__name__", __name__)

    ppp = Path(__file__)
    print(ppp.name)
    for qqq in ppp.parts:
        print(qqq)

if __name__ == "__main__":
    tester()
