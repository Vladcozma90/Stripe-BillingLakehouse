from airflow.sdk import dag, task

@dag(dag_id="first_dag")
def first_dag():

    @task.python
    def first_task():
        print("test1")

    @task.python
    def second_task():
        print("test_run was ran succesfully (I hope).")

    @task.python
    def third_task():
        print("Yes, it was a success, if not I believe I should've gotten an error and not get to this point..")

    @task.bash(task_id="fourth_task")
    def fourth_task():
        return "echo 'This is the fourth task, which is a bash task!'"

    first = first_task()
    second = second_task()
    third = third_task()
    fourth = fourth_task()

    first >> second >> third >> fourth


first_dag()