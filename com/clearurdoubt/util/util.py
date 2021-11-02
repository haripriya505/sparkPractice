from random import randint


def run():
    departments = ["CSE", "ECE", "EEE", "IT"]
    with open('../../../resources/students.json', 'w') as f:
        for i in range(0, 1000):
            f.write(
                f'{{"firstname":"hari{i}","lastname":"priya{i}","department":"{departments[randint(0, 3)]}","maths":{randint(0,100)},"science":{randint(0,100)},"english":{randint(0,100)}}}\n')


if __name__ == '__main__':
    run()
