def run():
    with open('../../../resources/students.json', 'w') as f:
        for i in range(0, 1000):
            f.write(
                f'{{"firstname":"hari{i}","lastname":"priya{i}","department":"CSE","maths":{i},"science":{i},"english":{i}}}\n')


if __name__ == '__main__':
    run()
