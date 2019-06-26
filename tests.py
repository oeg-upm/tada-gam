import unittest
import captain
import subprocess


# def start_services():
#     pass


class TestGam(unittest.TestCase):

    def test_start_services(self):
        subprocess.call(["docker-compose", "down"])
        captain.parse_args(["up", "--services", "score=3", "combine=2"])
        output = subprocess.check_output(["docker-compose", "ps"])
        self.assertEqual(7, len(output.strip().split('\n')))


if __name__ == '__main__':
    unittest.main()
