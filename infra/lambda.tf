data "archive_file" "zip" {
  type        = "zip"
  source_file = "../app/lambda_function.py"
  output_path = "../app/lambda_function.zip"
}

resource "aws_lambda_function" "dynamo_parallel_scan_python" {
  function_name    = var.project_name
  filename         = data.archive_file.zip.output_path
  source_code_hash = filebase64sha256(data.archive_file.zip.output_path)
  role             = var.role_arn
  runtime          = "python3.12"
  handler          = "lambda_auth.lambda_handler"
  timeout          = 10
}

resource "aws_cloudwatch_log_group" "convert_log_group" {
  name = "/aws/lambda/${aws_lambda_function.dynamo_parallel_scan_python.function_name}"
}

resource "aws_lambda_function_url" "lambda_url" {
  function_name      = aws_lambda_function.dynamo_parallel_scan_python.function_name
  authorization_type = "NONE"
}