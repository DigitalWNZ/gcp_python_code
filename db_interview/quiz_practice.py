from statistics import median
class Container:
    """
    A container of integers that should support
    addition, removal, and search for the median integer
    """
    def __init__(self):
        self._integer_list=[]
        pass

    def add(self, value: int) -> list:
        """
        Adds the specified value to the container

        :param value: int
        """
        # TODO: implement this method
        if isinstance(value,int):
            self._integer_list.append(value)
            return self._integer_list
        else:
            return self._integer_list

    def delete(self, value: int) -> bool:
        """
        Attempts to delete one item of the specified value from the container

        :param value: int
        :return: True, if the value has been deleted, or
                 False, otherwise.
        """
        # TODO: implement this method
        if value in self._integer_list:
            self._integer_list.remove(value)
            return True
        else:
            return False

    def get_median(self) -> int:
        """
        Finds the container's median integer value, which is
        the middle integer when the all integers are sorted in order.
        If the sorted array has an even length,
        the leftmost integer between the two middle
        integers should be considered as the median.

        :return: The median if the array is not empty, or
        :raise:  a runtime exception, otherwise.
        """
        # TODO: implement this method
        if len(self._integer_list) > 0:
            sorted_list=sorted(self._integer_list)
            len_list=len(self._integer_list)
            if len_list == 1:
                return sorted_list[0]
            elif (len_list % 2 ) == 0:
                return (sorted_list[len_list//2 - 1 ])
            else:
                return (sorted_list[(len_list-1)//2 ])
        else:
            raise Exception

'''
SQL test

1. 一个人一个座位一个request
2. 一个人一个座位多个request
3. 多个人一个座位多个request
4. 多个人多个座位多个request

思路：
1. 找出每个人每个座位的第一个请求（min request id）和最高的request（request），形成一条记录。 
2. 找出每个座位的第一个请求，直接更新。 

CREATE PROCEDURE solution()
BEGIN
    /* Write your SQL here. Terminate each statement with a semicolon. */
    with request_person_rank as (
        select 
            r.seat_no,
            r.person_id,
            min(request_id) as request_id,
            max(request) as request
        from requests r 
        group by 1,2
    ),
    request_rank as (
        select 
            r.*,
            row_number() over (partition by seat_no order by request_id) as rn
        from request_person_rank r 
    ),
    request_final as (
        select * from request_rank where rn = 1
    )
    select 
        s.seat_no,
        CASE
            when s.status = 0 and r.request = 1  then 1
            when s.status = 0 and r.request = 2  then 2
            when s.status = 1 and r.request=2 and s.person_id=r.person_id then 2
            else s.status
        End as status,
        CASE
            when s.status = 0 and r.request = 1  then r.person_id
            when s.status = 0 and r.request = 2  then r.person_id
            when s.status = 1 and r.request=2 and s.person_id=r.person_id then r.person_id
            else s.person_id
        End as person_id
    from seats s 
    left join request_final r
    on s.seat_no=r.seat_no;
END

'''