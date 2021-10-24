from functools import reduce

brackets = []
with open('input.txt') as file:
	for line in file:
		_, a, b = [float(v) for v in line.strip().split()]
		first = a + b
		second = a - 2*b
		brackets.append((a, b))
brackets = map(lambda x : x[0] * x[1], brackets)
result = reduce(lambda acc, x: acc + x, brackets)
print(result)
			
