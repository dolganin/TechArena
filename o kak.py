import itertools

# Функция для расчета количества строк после фильтрации по предикатам
def calculate_filtered_rows(rows, predicates, attr_cardinalities):
    for _, attr in predicates:
        if attr in attr_cardinalities:
            rows /= attr_cardinalities[attr]
    return rows

def calculate_cross_rows(left_rows, right_rows):
    return left_rows * right_rows

# Функция для расчета стоимости скана
def calculate_scan_cost(rows, predicates_count):
    return rows * (2 if predicates_count > 0 else 1)

# Функция для расчета стоимости NestLoop Join
def calculate_nestloop_cost(left_rows, right_rows, result_rows):
    return (left_rows - 1) * right_rows + result_rows * 0.1

# Функция для расчета стоимости HashJoin
def calculate_hashjoin_cost(left_rows, right_rows, result_rows):
    return right_rows * 1.5 + left_rows * 3.5 + result_rows * 0.1

def calculate_nestloop_cross(left_rows, right_rows):
    return right_rows * 0.2 + (left_rows - 1) * right_rows * 0.1

# Функция для расчета количества строк после джоина
def calculate_rows_after_join(left_rows, right_rows, join_cardinality):
    return (left_rows * right_rows) / join_cardinality



def parse_input(data):
    lines = data.splitlines()
    idx = 0
    
    # Считывание данных
    num_tables = int(lines[idx].strip())
    idx += 1
    
    table_sizes = list(map(int, lines[idx].split()))
    idx += 1
    
    attr_cardinalities = {}
    num_attributes = int(lines[idx].strip())
    idx += 1
    
    for _ in range(num_attributes):
        table_num, attr, cardinality = lines[idx].split()
        table_num, cardinality = int(table_num) - 1, int(cardinality)
        attr_cardinalities[(table_num, attr)] = cardinality
        idx += 1
    
    num_predicates = int(lines[idx].strip())
    idx += 1
    
    table_predicates = [[] for _ in range(num_tables)]
    for _ in range(num_predicates):
        table_num, attr = lines[idx].split()
        table_num = int(table_num) - 1
        table_predicates[table_num].append((table_num, attr))
        idx += 1
    
    num_join_predicates = int(lines[idx].strip())
    idx += 1
    
    join_predicates = []
    for _ in range(num_join_predicates):
        table1, table2, attr1, attr2 = lines[idx].split()
        join_predicates.append((int(table1) - 1, int(table2) - 1, attr1, attr2))
        idx += 1
    
    return num_tables, table_sizes, attr_cardinalities, table_predicates, join_predicates


def find_optimal_cross_join(tables, table_sizes, attr_cardinalities, table_predicates, memo):
    if len(tables) == 1:
        table = tables[0]
        predicates = table_predicates[table]
        rows = calculate_filtered_rows(table_sizes[table], predicates, attr_cardinalities)
        cost = calculate_scan_cost(rows, len(predicates))
        plan = f"{table + 1}{''.join(attr for _, attr in predicates)}"
        return plan, cost, rows

    # Используем кэширование для предотвращения пересчета
    if tables in memo:
        return memo[tables]

    best_cost = float('inf')
    best_plan = None
    best_result_rows = None
    
    for i in range(1, len(tables)):
        for left_tables in itertools.combinations(tables, i):
            right_tables = tuple(set(tables) - set(left_tables))
            
            left_plan, left_cost, left_rows = find_optimal_cross_join(left_tables, table_sizes, attr_cardinalities, table_predicates, memo)
            right_plan, right_cost, right_rows = find_optimal_cross_join(right_tables, table_sizes, attr_cardinalities, table_predicates, memo)
            
            join_cost = calculate_nestloop_cross(left_rows, right_rows)
            total_cost = left_cost + right_cost + join_cost
            
            # Обновление лучшего плана
            if total_cost < best_cost:
                best_cost = total_cost
                best_plan = f"({left_plan} {right_plan})"
                best_result_rows = calculate_cross_rows(left_rows, right_rows)
    
    memo[tables] = (best_plan, best_cost, best_result_rows)
    return best_plan, best_cost, best_result_rows




# Рекурсивная функция для нахождения оптимального джоина
def find_optimal_join(tables, table_sizes, attr_cardinalities, table_predicates, join_predicates, memo):
    if len(join_predicates) == 0:
        find_optimal_cross_join(tables, table_sizes, attr_cardinalities, table_predicates, memo)
    if len(tables) == 1:
        table = tables[0]
        predicates = table_predicates[table]
        rows = calculate_filtered_rows(table_sizes[table], predicates, attr_cardinalities)
        cost = calculate_scan_cost(rows, len(predicates))
        plan = f"{table + 1}{''.join(attr for _, attr in predicates)}"
        return plan, cost, rows

    # Используем кэширование для предотвращения пересчета
    if tables in memo:
        return memo[tables]

    best_cost = float('inf')
    best_plan = None
    best_result_rows = None
    
    for i in range(1, len(tables)):
        for left_tables in itertools.combinations(tables, i):
            right_tables = tuple(set(tables) - set(left_tables))
            
            left_plan, left_cost, left_rows = find_optimal_join(left_tables, table_sizes, attr_cardinalities, table_predicates, join_predicates, memo)
            right_plan, right_cost, right_rows = find_optimal_join(right_tables, table_sizes, attr_cardinalities, table_predicates, join_predicates, memo)
            
            join_attrs = [(table1, table2, attr1, attr2) for table1, table2, attr1, attr2 in join_predicates 
                          if (table1 in left_tables and table2 in right_tables) or (table2 in left_tables and table1 in right_tables)]
            
            if join_attrs:
                table1, table2, attr1, attr2 = join_attrs[0]
                
                # Проверяем наличие атрибутов в attr_cardinalities
                if (table1, attr1) not in attr_cardinalities or (table2, attr2) not in attr_cardinalities:
                    continue  # Пропускаем итерацию, если данные отсутствуют
                
                join_cardinality = max(attr_cardinalities[(table2, attr2)], attr_cardinalities[(table1, attr1)])
                result_rows = calculate_rows_after_join(left_rows, right_rows, join_cardinality)
                join_type = "HashJoin"
                join_clause = f"{{{table1 + 1}.{attr1} {table2 + 1}.{attr2}}}"
            else:
                result_rows = left_rows * right_rows
                join_type = "NestLoop"
                join_clause = ""
            
            # Выбор типа джоина и расчёт стоимости
            if join_type == "HashJoin":
                join_cost = calculate_hashjoin_cost(left_rows, right_rows, result_rows)
            else:
                join_cost = calculate_nestloop_cost(left_rows, right_rows, result_rows)
                
            total_cost = left_cost + right_cost + join_cost
            
            # Обновление лучшего плана
            if total_cost < best_cost:
                best_cost = total_cost
                best_plan = f"({left_plan} {right_plan} {join_clause})"
                best_result_rows = result_rows
    
    memo[tables] = (best_plan, best_cost, best_result_rows)
    return best_plan, best_cost, best_result_rows

def main():
    
    data = ""
    
    with open('input.txt', 'r') as file:
        data = file.read()
    # Передаем строку с данными
    num_tables, table_sizes, attr_cardinalities, table_predicates, join_predicates = parse_input(data)
    
    tables = tuple(range(num_tables))
    memo = {}
    plan, cost, _ = find_optimal_join(tables, table_sizes, attr_cardinalities, table_predicates, join_predicates, memo)
    print(f"{plan} {cost:.2f}")

if __name__ == "__main__":
    main()
