class UFDS:
    """
    Union-find disjoint set.
    """

    def __init__(self, N):
        """
        Sets up the useful arrays like the parent array and the rank array.
        """
        self.p, self.rank, self.set = [i for i in range(N)], [0 for _ in range(N)], [{i} for i in range(N)]

    def findSet(self, i):
        """
        Finds the representative set.
        """
        if self.p[i] == i: return i
        self.p[i] = self.findSet(self.p[i])
        return self.p[i]

    def isSameSet(self, i, j):
        """
        Checks if i and j are in the same set.
        """
        return self.findSet(i) == self.findSet(j)

    def union(self, i, j):
        """
        Merges the set containing i and the set containing j.
        """
        if not self.isSameSet(i, j):
            x, y = self.findSet(i), self.findSet(j)
            if self.rank[x] > self.rank[y]:
                self.p[y] = x
                self.set[x] |= self.set[y]
            else:
                self.p[x] = y
                self.set[y] |= self.set[x]
                if self.rank[x] == self.rank[y]: self.rank[y] += 1
