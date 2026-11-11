INSERT INTO ingredients (ingredient_id, name, unit, available_quantity, updated_at) VALUES
('GAO', 'Gạo', 'gram', 1000, now()),
('THIT_GA', 'Thịt gà', 'gram', 1500, now());

INSERT INTO recipe_items (dish_id, ingredient_id, quantity_per_dish) VALUES
('COM_GA', 'GAO', 100),
('COM_GA', 'THIT_GA', 150),
('CHAO_GA', 'GAO', 50),
('CHAO_GA', 'THIT_GA', 100);
