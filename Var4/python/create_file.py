import random
with open("input.txt", "wt") as f:

	for i in range(1000000):
		a = random.uniform(-50, 50)
		b = random.uniform(-50, 50)

		f.write(str(i) + '\t' + str(a) + '\t' + str(b) + '\n')