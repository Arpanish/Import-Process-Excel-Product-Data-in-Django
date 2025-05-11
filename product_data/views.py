import io
import pandas as pd
import logging
from django.db import transaction
from django.core.validators import FileExtensionValidator
from django.shortcuts import render

from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import api_view

from product_data.models import Product

logging.basicConfig(level=logging.INFO, filename='product_import.log', format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('product_import')

MANDATORY_FIELDS = ['id', 'title', 'description', 'link', 'image_link', 'availability', 'price', 'condition', 'brand', 'gtin']

def validate_row(row):
    errors = [f"{field} is required." for field in MANDATORY_FIELDS if pd.isna(row.get(field))]
    warnings = ["item_group_id is missing."] if pd.isna(row.get('item_group_id')) else []
    return errors, warnings

def process_data(df, chunk_size=100):
    results = {'success': 0, 'warnings': 0, 'errors': 0, 'log': []}
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        with transaction.atomic():
            for i, row in chunk.iterrows():
                errors, warnings = validate_row(row)
                row_log = {'row': i + 1, 'errors': errors, 'warnings': warnings}
                if errors:
                    results['errors'] += 1
                    logger.error(f"Row {i + 1} errors: {errors}")
                else:
                    try:
                        Product.objects.create(**row.dropna().to_dict())
                        results['success'] += 1
                        logger.info(f"Row {i + 1} successfully inserted.")
                    except Exception as e:
                        errors.append(f"Error creating product: {str(e)}")
                        results['errors'] += 1
                        logger.error(f"Row {i + 1} error: {errors}")
                if warnings:
                    results['warnings'] += 1
                    logger.warning(f"Row {i + 1} warnings: {warnings}")
                results['log'].append(row_log)
    return results


class ProductDataViewSet(viewsets.GenericViewSet):
    def create(self,request):
        file_data = request.FILES.get('file')
        if not file_data:
            return Response({"file": "This field is required."}, status=status.HTTP_404_NOT_FOUND)
        try:
            FileExtensionValidator(allowed_extensions=['xlsx', 'csv', 'xltm'])(file_data)
        except Exception:
            return Response({"error":"File extension validation failed, only excel files are supported"}, status=status.HTTP_404_NOT_FOUND)
        file_content = file_data.read()
        file_data.close()
        try:
            df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
        except Exception:
            df = pd.read_csv(io.BytesIO(file_content))
        results = process_data(df)
        return Response({
            "message": "File processed successfully.",
            "results": results
        }, status=status.HTTP_201_CREATED)