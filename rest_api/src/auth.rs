use rocket::fairing::{ self, Fairing, Info, Kind };
use rocket::request::Request;
use rocket::{Build, Orbit, Response, Rocket};

pub struct AuthFairing;

#[rocket::async_trait]
impl Fairing for AuthFairing {
    fn info(&self) -> Info {
        Info {
            name: "Logging Fairing",
            kind: Kind::Request | Kind::Response,
        }
    }


    async fn on_ignite(&self, rocket: Rocket<Build>) -> fairing::Result {/* ... */
        Ok(rocket)
    }

    async fn on_liftoff(&self, rocket: &Rocket<Orbit>) {/* ... */}

    async fn on_response<'r>(&self, req: &'r Request<'_>, res: &mut Response<'r>) {/* ... */}

    async fn on_shutdown(&self, rocket: &Rocket<Orbit>) {/* ... */}

    async fn on_request(&self, request: &mut Request<'_>, _data: &mut rocket::Data<'_>) {
        println!("Auth check for request: {}", request.uri());
        // You can perform additional logging or processing here for incoming requests.
    }

}
