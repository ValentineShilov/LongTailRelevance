with open("cqe.txt") as f:
	for line in f:
		line = line.strip()
		parts = line.split("\t", 2)

		if len(parts)>2:
			qid = int(parts[0])
			if qid>=1000000000:
				qid-=1000000000
			dist = int(parts[1])
			data = parts[2]
			with open("splitted/" + str(qid) + ".txt", "a") as g:
				g.write(data + "\n")
