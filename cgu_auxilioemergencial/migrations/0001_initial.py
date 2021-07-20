# Generated by Django 3.2.5 on 2021-07-20 19:59

from django.db import migrations, models
import django.db.models.deletion
import psqlextra.manager.manager
import uuid


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='InsertionTask',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('finished', models.BooleanField(default=False)),
                ('start', models.PositiveBigIntegerField()),
                ('end', models.PositiveBigIntegerField()),
                ('filepath', models.TextField()),
            ],
            managers=[
                ('objects', psqlextra.manager.manager.PostgresManager()),
            ],
        ),
        migrations.CreateModel(
            name='Person',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('reference_date', models.DateField(help_text='Data de referência do registro')),
                ('uf', models.TextField(help_text='Sigla da Unidade Federativa do beneficiário')),
                ('county_code', models.TextField(help_text='Código, no IBGE, do município do beneficiário')),
                ('county', models.TextField(help_text='Nome do município do beneficiário')),
                ('nis', models.TextField(help_text='Número de NIS do beneficiário')),
                ('cpf', models.TextField(help_text='Número no CPF do beneficiário')),
                ('name', models.TextField(help_text='Nome do beneficiário')),
                ('responsible_nis', models.TextField(help_text='Número de NIS do responsável pelo beneficiário')),
                ('responsible_cpf', models.TextField(help_text='Número no CPF do responsável pelo beneficiário')),
                ('responsible_name', models.TextField(help_text='Nome do responsável pelo beneficiário')),
                ('framework', models.TextField(help_text='Identifica se o beneficiário faz parte de algum grupo de programas sociais')),
                ('portion', models.IntegerField(help_text='Número sequencial da parcela disponibilizada')),
                ('observation', models.TextField(help_text='Indica alterações na parcela disponibilizada como, por exemplo, se foi devolvida ou está retida')),
                ('value', models.BigIntegerField(help_text='Valor disponibilizado na parcela')),
            ],
            managers=[
                ('objects', psqlextra.manager.manager.PostgresManager()),
            ],
        ),
        migrations.CreateModel(
            name='Release',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('date', models.DateField(unique=True)),
                ('finished', models.BooleanField(default=False)),
                ('uri', models.TextField()),
                ('folder', models.TextField()),
            ],
            managers=[
                ('objects', psqlextra.manager.manager.PostgresManager()),
            ],
        ),
        migrations.AddIndex(
            model_name='release',
            index=models.Index(fields=['created_at'], name='cgu_auxilio_created_ae8ec8_idx'),
        ),
        migrations.AddIndex(
            model_name='release',
            index=models.Index(fields=['updated_at'], name='cgu_auxilio_updated_0a691a_idx'),
        ),
        migrations.AddIndex(
            model_name='release',
            index=models.Index(fields=['finished'], name='cgu_auxilio_finishe_a22061_idx'),
        ),
        migrations.AddIndex(
            model_name='person',
            index=models.Index(fields=['created_at'], name='cgu_auxilio_created_c9cccc_idx'),
        ),
        migrations.AddIndex(
            model_name='person',
            index=models.Index(fields=['updated_at'], name='cgu_auxilio_updated_884e0b_idx'),
        ),
        migrations.AddIndex(
            model_name='person',
            index=models.Index(fields=['reference_date'], name='cgu_auxilio_referen_157b98_idx'),
        ),
        migrations.AddIndex(
            model_name='person',
            index=models.Index(fields=['uf'], name='cgu_auxilio_uf_de8980_idx'),
        ),
        migrations.AddIndex(
            model_name='person',
            index=models.Index(fields=['county_code'], name='cgu_auxilio_county__07ce18_idx'),
        ),
        migrations.AddIndex(
            model_name='person',
            index=models.Index(fields=['nis'], name='cgu_auxilio_nis_00bf59_idx'),
        ),
        migrations.AddIndex(
            model_name='person',
            index=models.Index(fields=['cpf'], name='cgu_auxilio_cpf_9f835f_idx'),
        ),
        migrations.AddIndex(
            model_name='person',
            index=models.Index(fields=['name'], name='cgu_auxilio_name_937dfa_idx'),
        ),
        migrations.AddIndex(
            model_name='person',
            index=models.Index(fields=['responsible_nis'], name='cgu_auxilio_respons_a508ab_idx'),
        ),
        migrations.AddIndex(
            model_name='person',
            index=models.Index(fields=['responsible_cpf'], name='cgu_auxilio_respons_06bfca_idx'),
        ),
        migrations.AddIndex(
            model_name='person',
            index=models.Index(fields=['responsible_name'], name='cgu_auxilio_respons_89fc6d_idx'),
        ),
        migrations.AddIndex(
            model_name='person',
            index=models.Index(fields=['value'], name='cgu_auxilio_value_d6c942_idx'),
        ),
        migrations.AddConstraint(
            model_name='person',
            constraint=models.UniqueConstraint(fields=('reference_date', 'uf', 'county_code', 'nis', 'cpf', 'name', 'responsible_nis', 'responsible_cpf', 'responsible_name', 'framework', 'portion', 'observation', 'value'), name='unique_cgu_auxilioemergencial_person'),
        ),
        migrations.AddField(
            model_name='insertiontask',
            name='release',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='cgu_auxilioemergencial.release'),
        ),
        migrations.AddIndex(
            model_name='insertiontask',
            index=models.Index(fields=['created_at'], name='cgu_auxilio_created_6f0243_idx'),
        ),
        migrations.AddIndex(
            model_name='insertiontask',
            index=models.Index(fields=['updated_at'], name='cgu_auxilio_updated_404f83_idx'),
        ),
        migrations.AddIndex(
            model_name='insertiontask',
            index=models.Index(fields=['finished'], name='cgu_auxilio_finishe_97f477_idx'),
        ),
    ]