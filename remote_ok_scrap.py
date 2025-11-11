import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import time
import re

class RemoteOkScraper:
    def __init__(self) -> None:
        self.base_url = "https://remoteok.com/api"
        self.headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.exclude_terms = ['senior', r'\bsr\.\b', r'\bsr\b', 'lead', 'principal', 'manager', 'director', 'head', 'staff', 'executive']
        self.intern_tokens =  ['intern', 'internship', 'student', 'graduate', 'junior', 'entry-level', 'entry level', 'jr.', 'jr']
    
    def _normalize_list(self, maybe_list):
        if not maybe_list:
            return []
        return [str(x).lower() for x in maybe_list]
    
    
    def _whole_word_search(self, text, phrase):
        if not text or not phrase:
            return False
        pattern = r'\b' + re.escape(phrase.lower()) + r'\b'
        return re.search(pattern, text.lower()) is not None
    
    def _any_phrase_in(self, text, phrases) -> bool:
        text_l = (text or "").lower()
        for p in phrases:
            try:
                if re.search(p if p.startswith(r'\b') else r'\b' + re.escape(p) + r'\b', text_l):
                    return True
            except re.error:
                if p in text_l:
                    return True
        return False
    
    def scrape_jobs(self, keywords=None, min_keywords_match=2,junior_only=True, require_skill_match = True):
        print("scraping remoteok ...")
        
        try:
            response = requests.get(self.base_url, headers=self.headers, timeout=15)
            response.raise_for_status()
            jobs_data = response.json()
            jobs_data = jobs_data[1:] if isinstance(jobs_data, list) and len(jobs_data) > 1 else []

            # normalize keywords and separate skill keywords from level keywords
            keywords = [k.lower() for k in (keywords or [])]
            skill_keywords = [k for k in keywords if k not in self.intern_tokens]
            level_keywords = [k for k in keywords if k in self.intern_tokens]

            filtered_jobs = []

            for job in jobs_data:
                title = (job.get('position') or job.get('title') or '').strip()
                company = (job.get('company') or '').strip()
                description = job.get('description') or ''
                tags = self._normalize_list(job.get('tags', []))
                location = job.get('location') or job.get('location_names') or 'Remote'
                slug = job.get('slug') or str(job.get('id') or '')
                date_posted = job.get('date') or job.get('created_at') or ''

                search_text = " ".join([title, company, description, " ".join(tags)]).lower()

                level_in_title = any(self._whole_word_search(title, tok) for tok in self.intern_tokens)
                level_in_description = any(self._whole_word_search(description, tok) for tok in self.intern_tokens)
                level_in_company = any(self._whole_word_search(company, tok) for tok in self.intern_tokens)
                strong_level = level_in_title or level_in_description or level_in_company

                level_in_tags = any(tok in tags for tok in self.intern_tokens)
                weak_level = (level_in_tags and not strong_level)

                senior_in_title = any(self._whole_word_search(title, t) for t in self.exclude_terms)
                senior_in_description = any(self._whole_word_search(description, t) for t in self.exclude_terms)
                senior_in_company = any(self._whole_word_search(company, t) for t in self.exclude_terms)
                senior_in_tags = any(t in tags for t in self.exclude_terms)
                senior_strong = senior_in_title or senior_in_description or senior_in_company
                senior_weak = senior_in_tags and not senior_strong

                matches = set()
                skill_matches = set()
                level_matches = set()
                for kw in keywords:
                    if self._whole_word_search(title, kw):
                        matches.add((kw, 'title'))
                    if self._whole_word_search(description, kw):
                        matches.add((kw, 'description'))
                    if kw in tags:
                        matches.add((kw, 'tag'))
                    if self._whole_word_search(company, kw):
                        matches.add((kw, 'company'))

                for (kw, where) in matches:
                    if kw in self.intern_tokens:
                        level_matches.add((kw, where))
                    else:
                        skill_matches.add((kw, where))

                num_matches = len({kw for kw, _ in matches})

                if senior_strong and not strong_level:
                    why = ['rejected:senior_in_title_or_desc_and_no_strong_level']
                    continue

                accepted = False
                why_reasons = []

                if junior_only:
                    if strong_level:
                        accepted = True
                        why_reasons.append('accepted:strong_level_in_title_or_description_or_company')
                    else:
                        if level_matches and skill_matches:
                            accepted = True
                            why_reasons.append(f'accepted:level_keyword_and_skill_keyword_matches ({sorted({k for k,_ in level_matches})}, skills={sorted({k for k,_ in skill_matches})})')
                        elif weak_level:
                            if level_matches and (not require_skill_match or skill_matches):
                                accepted = True
                                why_reasons.append('accepted:weak_level_from_tags_but_keywords_support')
                            else:
                                # Not enough evidence
                                accepted = False
                                why_reasons.append('rejected:weak_level_only_in_tags_and_no_skill_match')
                        else:
                            accepted = False
                            why_reasons.append('rejected:no_level_signal')
                else:
                    if num_matches >= min_keywords_match:
                        accepted = True
                        why_reasons.append(f'accepted:keyword_matches={num_matches}')

                if senior_strong and not strong_level:
                    accepted = False
                    why_reasons.append('final_reject:senior_strong_and_no_strong_level')
                    
                    
                if accepted:
                    job_info = {
                        'title': job.get('position'),
                        'company': job.get('company'),
                        'location': job.get('location', 'Remote'),
                        'url': f"https://remoteok.com/remote-jobs/{job.get('slug', '')}",
                        'date_posted': job.get('date'),
                        'tags': job.get('tags', []),
                        'description': job.get('description', '')[:500],
                        'salary': f"${job.get('salary_min', 'N/A')}-${job.get('salary_max', 'N/A')}" if job.get('salary_min') else 'Not specified',
                        'source': 'RemoteOK',
                        'scraped_at': datetime.now().isoformat()+'Z',
                        'skill_matches': sorted({k for k, _ in skill_matches}),
                        'level_matches': sorted({k for k, _ in level_matches})
                    }
                    filtered_jobs.append(job_info)
                    
            print(f"Found {len(filtered_jobs)} relevant jobs from RemoteOK")
            return filtered_jobs
        
        except Exception as e:
            print(f"Error scraping RemoteOK: {e}")
            return []
    
    def save_to_json(self,jobs, filename='remoteok_jobs.json'):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(jobs, f, indent=2, ensure_ascii= False)
        print(f"Saved {len(jobs)} jobs to {filename}")
        

if __name__ == "__main__":
    scraper = RemoteOkScraper()
    
    keywords = ['AI Engineer', 'Frontend', 'Backend', 'Machine Learning', 'Python', 'React', 'Junior', 'intern']

    jobs = scraper.scrape_jobs(keywords=keywords, min_keywords_match= 2)
    
    for i, job in enumerate(jobs[:3], 1):
        print(f"{i}. {job['title']} at {job['company']}")
        print(f"   Tags: {', '.join(job['tags'][:5])}")
        print(f"   URL: {job['url']}\n")
        
    scraper.save_to_json(jobs)
    
    print(f"\n Total jobs found: {len(jobs)}")
    print("Check 'remoteok_jobs.json' for full results")
        