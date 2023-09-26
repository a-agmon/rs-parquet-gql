import http from 'k6/http';
import { check } from 'k6';
import { sleep } from 'k6';

const query = `
query GetUsersByEmail {
    getUsersByEmail(email: "*******") {
        user_id
        acc_id
        email
        department
        created_at
    }
}
`;

export const options = {
    vus: 75,
    duration: '10s',
};
export default function () {
    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };
    const res = http.post('http:/******:8080/query', JSON.stringify({ query: query }), params);
    //const res = http.post('http://localhost:8999/query', JSON.stringify({ query: query }), params);
    //console.log(res.body);
    check(res, {
        'verify  user id': (r) =>
            r.body.includes('0037U0000044S9qQAE'),
    });
}
// this was run using k6 run load_test.js
