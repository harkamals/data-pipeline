import luigi
from time import sleep


class PrintNumbers(luigi.Task):
    n = luigi.IntParameter()

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget("numbers_{}.txt".format(self.n))

    def run(self):
        print("Running: PrintNumbers")
        with self.output().open('w') as f:
            for i in range(1, self.n + 1):
                f.write("{}\n".format(i))


class SquaredNumbers(luigi.Task):
    n = luigi.IntParameter()

    def requires(self):
        return [PrintNumbers(n=self.n)]

    def output(self):
        return luigi.LocalTarget("squares_{}.txt".format(self.n))

    def run(self):
        print("Running: Squared numbers")
        with self.input()[0].open() as fin, self.output().open('w') as fout:
            for line in fin:
                n = int(line.strip())
                out = n * n
                fout.write("{}:{}\n".format(n, out))


def luigi_handler():
    print("Luigi Handler")

    # Callable
    luigi.run(['SquaredNumbers', '--workers', '2', '--local-scheduler', '--n', '5'])

    # if luigid is running
    # luigi.run(['SquaredNumbers', '--workers', '2', '--n', '5'])


if __name__ == "__main__":
    luigi.run()
