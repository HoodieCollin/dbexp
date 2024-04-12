use rocket::fairing::{ Fairing, Info, Kind };
use rocket::request::Request;

pub struct AuthFairing;

#[rocket::async_trait]
impl Fairing for AuthFairing {
    fn info(&self) -> Info {
        Info {
            name: "Logging Fairing",
            kind: Kind::Request | Kind::Response,
        }
    }

    async fn on_request(&self, request: &mut Request<'_>, _data: &mut rocket::Data<'_>) {
        println!("Auth check for request: {}", request.uri());
        // You can perform additional logging or processing here for incoming requests.
    }

}
