import 'dotenv/config';
import GoogleConnector from '.';

const connector = new GoogleConnector('Google');
//console.log(await connector.listTables(['file', '1u4mrDQQYE_yXxZwnWNvwfO80t4uWfviBXRPO0FXiq8Y']));
console.log(
    await connector.getTable(['file', '1u4mrDQQYE_yXxZwnWNvwfO80t4uWfviBXRPO0FXiq8Y', 'Recipes'])
);

console.log(
    await connector.getData!(
        await connector.getTable([
            'file',
            '1u4mrDQQYE_yXxZwnWNvwfO80t4uWfviBXRPO0FXiq8Y',
            'Recipes'
        ])
    )
);
