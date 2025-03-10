# Klimadashboard Flow: Data Pipeline Repository

This repository contains scripts for processing data for the [Klimadashboard](https://klimadashboard.org) project. It also includes helper scripts for translations, news, and other functionalities, identified by the `klimadashboard-` file prefix.

Scripts are categorized as:

- **Automated** (`automated/`): Scheduled tasks run via `crontab` on our data server.
- **Manual** (`manual/`): Scripts that require manual execution.

For inquiries, contact [David](mailto:david@klimadashboard.org).

## Contributing

We welcome bug reports and pull requests. If you're interested in joining our team of developers, researchers, and scientists, reach out at [team@klimadashboard.org](mailto:team@klimadashboard.org).

## Technologies

We mostly use Python scripts and occasionally Node.js for our data processing. Our backend is based on Directus and a Postgres database, our frontend is written in SvelteKit.

## Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/klimadashboard/flow.git
   ```
2. Navigate to the directory:
   ```bash
   cd flow
   ```
3. Install dependencies. These differ for each script.
4. Set up environmental variables. These are stored in a .env file. .env.example is provided for reference.

## License

This project is licensed under the [MIT License](LICENSE).
