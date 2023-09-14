import random

if __name__ == '__main__':

    for number_of_days in range(1,60):
        with open('formular_no_answer.txt', 'a') as f:
            f.write('----------------{}----------------\n'.format(str(number_of_days)))
            f.write("                    用时：\n")
            f.write("                    准确率：\n")
        with open('formular_with_answer.txt', 'a') as f:
            f.write('----------------{}----------------\n'.format(str(number_of_days)))
        for i in range(0,5):
            four_digit_left=random.randint(1000,9999)
            four_digit_right = random.randint(1000, 9999)
            four_digit_result = four_digit_left * four_digit_right
            four_digit_formular="{} X {} = ".format(str(four_digit_left),str(four_digit_right))
            four_digit_formular_result = "{} X {} = {}".format(str(four_digit_left),str(four_digit_right),str(four_digit_result))
            with open('formular_no_answer.txt','a') as f:
                f.write('{}\n'.format(four_digit_formular))
            with open('formular_with_answer.txt','a') as f:
                f.write('{}\n'.format(four_digit_formular_result))
        with open('formular_no_answer.txt','a') as f:
            f.write('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')
            f.write("                    用时：\n")
            f.write("                    准确率：\n")

        for i in range(0,3):
            tri_digit_left=random.randint(100,999)
            if random.random()>=0.5:
                operator_left='+'
                tri_digit_middle = random.randint(100,999)
                interim_result = tri_digit_left + tri_digit_middle
            else:
                operator_left = '-'
                tri_digit_middle = random.randint(100, tri_digit_left)
                interim_result = tri_digit_left - tri_digit_middle



            if random.random()>=0.5:
                operator_right = '+'
                tri_digit_right = random.randint(100, 999)
                tri_digit_formular_result = interim_result + tri_digit_right

            else:
                operator_right = '-'
                if interim_result > 1000:
                    tri_digit_right = random.randint(100, 999)
                elif interim_result >=100:
                    tri_digit_right = random.randint(100, interim_result)
                else:
                    tri_digit_right = random.randint(0,interim_result)

                tri_digit_formular_result = interim_result - tri_digit_right


            tri_digit_result = tri_digit_left * tri_digit_right
            tri_digit_formular="{} {} {} {} {}  = ".format(str(tri_digit_left),str(operator_left),str(tri_digit_middle),str(operator_right),str(tri_digit_right))
            tri_digit_formular_result="{} {} {} {} {}  =  {}".format(str(tri_digit_left),str(operator_left),str(tri_digit_middle),str(operator_right),str(tri_digit_right),str(tri_digit_formular_result))
            with open('formular_no_answer.txt', 'a') as f:
                f.write('{}\n'.format(tri_digit_formular))
            with open('formular_with_answer.txt', 'a') as f:
                f.write('{}\n'.format(tri_digit_formular_result))
        with open('formular_no_answer.txt', 'a') as f:
            f.write('\n')
        # with open('formular_with_answer.txt', 'a') as f:
        #     f.write('--------------------------------\n')

