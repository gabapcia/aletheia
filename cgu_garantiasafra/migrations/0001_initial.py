# Generated by Django 3.2.5 on 2021-07-22 17:46

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
            name='Release',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('date', models.DateField(unique=True)),
                ('finished', models.BooleanField(default=False)),
                ('folder', models.TextField()),
                ('uri', models.TextField()),
            ],
            managers=[
                ('objects', psqlextra.manager.manager.PostgresManager()),
            ],
        ),
        migrations.CreateModel(
            name='Warranty',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('reference_date', models.DateField(help_text='Ano/Mês da folha de pagamento')),
                ('uf', models.TextField(help_text='Sigla da Unidade Federativa do beneficiário do Garantia Safra')),
                ('county_siafi_code', models.TextField(help_text='Código, no SIAFI, do município do beneficiário do Garantia Safra')),
                ('county', models.TextField(help_text='Nome do município do beneficiário do Garantia Safra')),
                ('recipient_nis', models.TextField(help_text='NIS do beneficiário do Garantia Safra')),
                ('recipient_name', models.TextField(help_text='Nome do beneficiário do Garantia Safra')),
                ('value', models.BigIntegerField(help_text='Valor da parcela do benefício')),
            ],
            managers=[
                ('objects', psqlextra.manager.manager.PostgresManager()),
            ],
        ),
        migrations.AddIndex(
            model_name='warranty',
            index=models.Index(fields=['created_at'], name='cgu_garanti_created_7f9eca_idx'),
        ),
        migrations.AddIndex(
            model_name='warranty',
            index=models.Index(fields=['updated_at'], name='cgu_garanti_updated_974238_idx'),
        ),
        migrations.AddIndex(
            model_name='warranty',
            index=models.Index(fields=['reference_date'], name='cgu_garanti_referen_6fafec_idx'),
        ),
        migrations.AddIndex(
            model_name='warranty',
            index=models.Index(fields=['uf'], name='cgu_garanti_uf_eecbb8_idx'),
        ),
        migrations.AddIndex(
            model_name='warranty',
            index=models.Index(fields=['county_siafi_code'], name='cgu_garanti_county__818c5f_idx'),
        ),
        migrations.AddIndex(
            model_name='warranty',
            index=models.Index(fields=['recipient_nis'], name='cgu_garanti_recipie_1c5511_idx'),
        ),
        migrations.AddIndex(
            model_name='warranty',
            index=models.Index(fields=['recipient_name'], name='cgu_garanti_recipie_f23523_idx'),
        ),
        migrations.AddIndex(
            model_name='warranty',
            index=models.Index(fields=['value'], name='cgu_garanti_value_298356_idx'),
        ),
        migrations.AddConstraint(
            model_name='warranty',
            constraint=models.UniqueConstraint(fields=('reference_date', 'uf', 'county_siafi_code', 'recipient_nis', 'recipient_name', 'value'), name='unique_cgu_garantiasafra_warranty'),
        ),
        migrations.AddIndex(
            model_name='release',
            index=models.Index(fields=['created_at'], name='cgu_garanti_created_2fe552_idx'),
        ),
        migrations.AddIndex(
            model_name='release',
            index=models.Index(fields=['updated_at'], name='cgu_garanti_updated_edf6d4_idx'),
        ),
        migrations.AddIndex(
            model_name='release',
            index=models.Index(fields=['finished'], name='cgu_garanti_finishe_30e1e2_idx'),
        ),
        migrations.AddField(
            model_name='insertiontask',
            name='release',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='cgu_garantiasafra.release'),
        ),
        migrations.AddIndex(
            model_name='insertiontask',
            index=models.Index(fields=['created_at'], name='cgu_garanti_created_8b2c94_idx'),
        ),
        migrations.AddIndex(
            model_name='insertiontask',
            index=models.Index(fields=['updated_at'], name='cgu_garanti_updated_cae3c1_idx'),
        ),
        migrations.AddIndex(
            model_name='insertiontask',
            index=models.Index(fields=['finished'], name='cgu_garanti_finishe_9642c8_idx'),
        ),
    ]
