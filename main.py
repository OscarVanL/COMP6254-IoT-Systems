import click
from labs import Lab2
from coursework import CourseworkClient


@click.command()
@click.option("--coursework", is_flag=True, help="Launch the CourseworkClient MQTT code")
@click.option("--lab", required=False, type=int, help="Which lab's MQTT code to run [2]")
def start_IoT_lab(coursework, lab):

    if coursework:
        print("Launching CourseworkClient")
        CourseworkClient.CourseworkClient()

    if lab == 2:
        print("Launching Lab 2")
        Lab2.Lab2()

    raise ValueError("Either --coursework or --lab [lab no.] must be set.")


if __name__ == '__main__':
    start_IoT_lab()
