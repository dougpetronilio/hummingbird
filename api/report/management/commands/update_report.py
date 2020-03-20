import json

from datetime import datetime

import requests

from apscheduler.schedulers.blocking import BlockingScheduler
from django.core.management.base import BaseCommand
from django.utils.timezone import make_aware
from django.conf import settings
import boto3

from api.report.models import *

url = "http://plataforma.saude.gov.br/novocoronavirus/resources/scripts/database.js"


def cron(*args, **options):
    if 6 <= datetime.now().hour <= 20:

        print(f"Cron job is running. The time is {datetime.now()}")

        s3resource = boto3.resource('s3',
                                    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

        bucket_name = settings.AWS_STORAGE_BUCKET_NAME
        url = "%s%s"%(settings.BINO_URL, '/crawl/ministerio_saude_brasil')
        response = requests.post(url = url, data = {})
        file_name = json.loads(response.content.decode('utf-8'))['path']

        obj = s3resource.Object(bucket_name,"ministerio_saude_brasil/2020-03-20/16-47/rawData.json")

        content = obj.get()['Body'].read().decode('utf-8')
        data = json.loads(content)

        print('Object body: {}'.format(data['brazil']))

        for record in data['brazil']:
            date_time = datetime.strptime(f"{record['date']} {record['time']}",
                                          '%d/%m/%Y %H:%M')

            report, report_created = Report.objects.get_or_create(
                updated_at=make_aware(date_time)
            )

            if report_created:
                for value in record['values']:
                    state = int(value['uid'])

                    suspects = value.get('suspects', 0)
                    refuses = value.get('refuses', 0)
                    cases = value.get('cases', 0)
                    deaths = value.get('deaths', 0)
                    recovered = value.get('recovered', 0)

                    Case.objects.get_or_create(
                        suspects=suspects, refuses=refuses, cases=cases, deaths=deaths, recovered=recovered,
                        defaults={
                            'state': state,
                            'report': report
                        })

    print(f"Done! The time is: {datetime.now()}")


class Command(BaseCommand):
    help = 'Update kaggle dataset with the last cases of COVID-19 in Brazil.'

    def handle(self, *args, **options):
        print('Cron started! Wait the job starts!')

        scheduler = BlockingScheduler()
        scheduler.add_job(cron, 'interval', minutes=1, timezone=settings.TIME_ZONE)

        scheduler.start()
