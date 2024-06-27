import docker
import time
import subprocess
import sys

class ResultsChecker:

    def __init__(self):
        self.q1_res = []
        self.q2_res = 0
        self.q3_res = []
        self.q4_res = []
        self.q5_res = 0

    def run(self):
        self.get_state()

        cl_q1, cl_q2, cl_q3, cl_q4, cl_q5 = self.get_client_results(1)
        ok = self.check_client_results(cl_q1, cl_q2, cl_q3, cl_q4, cl_q5)

        if not ok:
            print("Error on Client 1")
            return 1

        cl_q1, cl_q2, cl_q3, cl_q4, cl_q5 = self.get_client_results(2)
        ok = self.check_client_results(cl_q1, cl_q2, cl_q3, cl_q4, cl_q5)

        if not ok:
            print("Error on Client 2")
            return 1
        
        cl_q1, cl_q2, cl_q3, cl_q4, cl_q5 = self.get_client_results(3)
        ok = self.check_client_results(cl_q1, cl_q2, cl_q3, cl_q4, cl_q5)

        if not ok:
            print("Error on Client 3")
            return 1

        print('All results OK')

        return 0

    def get_state(self):
        with open(f'./debug/results.txt', 'r') as f:
            lines = f.readlines()
            curr_q = 1
            for line in lines:
                if line == '[QUERY 1]\n':
                    curr_q = 1
                elif line == '[QUERY 2]\n':
                    curr_q = 2
                elif line == '[QUERY 3]\n':
                    curr_q = 3
                elif line == '[QUERY 4]\n':
                    curr_q = 4
                elif line == '[QUERY 5]\n':
                    curr_q = 5
                elif curr_q == 1:
                    self.q1_res.append(line)
                elif curr_q == 2:
                    authors_quantity = len(line.split(','))
                    self.q2_res += authors_quantity
                elif curr_q == 3:
                    self.q3_res.append(line)
                elif curr_q == 4:
                    self.q4_res.append(line)
                elif curr_q == 5:
                    titles_quantity =  len(line.split(','))
                    self.q5_res += titles_quantity

    def get_client_results(self, client_id):
        with open(f'./debug/results_{client_id}.txt', 'r') as f:
            lines = f.readlines()
            cl_q1 = []
            cl_q2 = 0
            cl_q3 = []
            cl_q4 = []
            cl_q5 = 0

            curr_q = 1
            for line in lines:
                if line == '[QUERY 1]\n':
                    curr_q = 1
                elif line == '[QUERY 2]\n':
                    curr_q = 2
                elif line == '[QUERY 3]\n':
                    curr_q = 3
                elif line == '[QUERY 4]\n':
                    curr_q = 4
                elif line == '[QUERY 5]\n':
                    curr_q = 5
                elif curr_q == 1:
                    cl_q1.append(line)
                elif curr_q == 2:
                    authors_quantity = len(line.split(','))
                    cl_q2 += authors_quantity
                elif curr_q == 3:
                    cl_q3.append(line)
                elif curr_q == 4:
                    cl_q4.append(line)
                elif curr_q == 5:
                    titles_quantity =  len(line.split(','))
                    cl_q5 += titles_quantity

        return cl_q1, cl_q2, cl_q3, cl_q4, cl_q5
    
    def check_client_results(self, cl_q1, cl_q2, cl_q3, cl_q4, cl_q5):
        # Check results from Q1
        if len(cl_q1) != len(self.q1_res):
            print("ERROR ON Q1")
            return False

        for line in cl_q1:
            if line not in self.q1_res:
                print("ERROR ON Q1")
                return False
            
        # Check results from Q2
        if self.q2_res != cl_q2:
            print("ERROR ON Q2")
            return False
        
        # Check results from Q3
        if len(cl_q3) != len(self.q3_res):
            print("ERROR ON Q3")
            return False

        for line in cl_q3:
            if line not in self.q3_res:
                print("ERROR ON Q3")
                return False
            
        # Check results from Q4
        if len(cl_q4) != len(self.q4_res):
            print("ERROR ON Q4")
            return False

        for line in cl_q4:
            if line not in self.q4_res:
                print("ERROR ON Q4")
                return False
            
        # Check results from Q5
        if self.q5_res != cl_q5:
            print("ERROR ON Q5")
            return False
        
        return True

if __name__ == "__main__":
    bg = ResultsChecker()
    
    exit_code = bg.run()
    sys.exit(exit_code)       